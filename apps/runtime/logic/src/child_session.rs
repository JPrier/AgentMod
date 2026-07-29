//! Runtime-managed child-session creation and orphan recovery.
//!
//! A worker is a fresh session with a typed parent/task link. It is not a
//! history branch and does not fabricate a user conversation entry.

use std::path::PathBuf;

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{CausationId, ContentHash, Sequence, SessionId, Version};
use agentmod_runtime_data::{
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
    registry::{
        BranchEventDataRecord, CreateChildSessionDataRequest, ListSessionsDataRequest,
        PrepareSessionDataRequest, SessionRegistryDataPort,
    },
    style::SessionStyleDataPort,
};
use thiserror::Error;

use crate::{
    RuntimeLogic,
    persistence::{LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort},
    session::{
        ChildSessionLinkedEvent, RuntimeCommittedEvent, SessionCreatedEvent, SessionStyleBinding,
    },
    style::{InspectStyleCommand, SessionStyleLogicError, SessionStyleLogicPort, StyleEnvironment},
};

const MAX_CHILD_RECOVERY_SCAN: usize = 1_000;

/// Logic-owned request to create or recover one exact worker session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureChildSessionCommand {
    /// Sessions storage root.
    pub sessions_root: PathBuf,
    /// Runtime-managed parent session.
    pub parent_session_id: SessionId,
    /// Canonical parent creation proposal sequence.
    pub parent_action_sequence: Sequence,
    /// Parent graph node that owns the worker.
    pub parent_graph_node_id: String,
    /// Parent-selected workspace.
    pub workspace: String,
    /// Exact child style selector.
    pub style_selector: String,
    /// Runtime-owned task identity.
    pub task_id: String,
    /// Zero-based task revision.
    pub revision: u32,
    /// One-based child depth.
    pub depth: u32,
    /// Bounded typed task input.
    pub task: String,
    /// Hard worker token budget.
    pub token_budget: u64,
}

/// Logic-owned child session identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildSessionResult {
    /// Runtime-managed worker session.
    pub session_id: SessionId,
    /// Exact parent proposal used for recovery.
    pub parent_action_sequence: Sequence,
    /// Selected child style.
    pub style: String,
    /// Whether an existing exact worker was reconciled.
    pub recovered: bool,
}

/// Narrow child-session business boundary consumed by style execution.
pub trait ChildSessionLogicPort: Send + Sync {
    /// Creates or reconciles the exact worker for one parent proposal.
    ///
    /// # Errors
    ///
    /// Returns [`ChildSessionLogicError`] for invalid identity, unavailable
    /// style, ambiguous recovery, or atomic creation failure.
    fn ensure_child_session(
        &self,
        command: EnsureChildSessionCommand,
    ) -> Result<ChildSessionResult, ChildSessionLogicError>;
}

/// Real child-session coordinator over runtime data ports.
#[derive(Clone)]
pub struct RuntimeChildSessionLogic<D> {
    data: D,
    environment: StyleEnvironment,
}

impl<D> RuntimeChildSessionLogic<D> {
    /// Constructs child coordination with an explicit style environment.
    #[must_use]
    pub const fn new(data: D, environment: StyleEnvironment) -> Self {
        Self { data, environment }
    }
}

impl<D> ChildSessionLogicPort for RuntimeChildSessionLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + SessionRegistryDataPort
        + SessionStyleDataPort,
{
    #[allow(
        clippy::too_many_lines,
        reason = "child recovery keeps exact catalog identity, style binding, canonical events, and atomic creation adjacent"
    )]
    fn ensure_child_session(
        &self,
        command: EnsureChildSessionCommand,
    ) -> Result<ChildSessionResult, ChildSessionLogicError> {
        validate(&command)?;
        let matches = self
            .data
            .list(ListSessionsDataRequest {
                sessions_root: command.sessions_root.clone(),
                limit: MAX_CHILD_RECOVERY_SCAN,
            })
            .map_err(ChildSessionLogicError::Registry)?
            .into_iter()
            .filter(|record| {
                record.child_parent_session_id == Some(command.parent_session_id)
                    && record.child_parent_action_sequence
                        == Some(command.parent_action_sequence.get())
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [child] => {
                if child.style != child_style_id(&command.style_selector)
                    || child.child_task_id.as_deref() != Some(command.task_id.as_str())
                {
                    return Err(ChildSessionLogicError::RecoveryIdentityMismatch);
                }
                let loaded = SessionPersistenceLogic::new(self.data.clone())
                    .load_session(LoadSessionCommand {
                        session_directory: command.sessions_root.join(child.id.to_string()),
                        expected_session_id: child.id,
                    })
                    .map_err(ChildSessionLogicError::Persistence)?;
                let Some(origin) = loaded.state.child_origin.as_ref() else {
                    return Err(ChildSessionLogicError::RecoveryIdentityMismatch);
                };
                if origin.parent_session_id != command.parent_session_id
                    || origin.parent_action_sequence != command.parent_action_sequence
                    || origin.parent_graph_node_id != command.parent_graph_node_id
                    || origin.task_id != command.task_id
                    || origin.revision != command.revision
                    || origin.depth != command.depth
                    || origin.task != command.task
                    || origin.input_hash != ContentHash::digest(command.task.as_bytes())
                    || origin.token_budget != command.token_budget
                {
                    return Err(ChildSessionLogicError::RecoveryIdentityMismatch);
                }
                return Ok(ChildSessionResult {
                    session_id: child.id,
                    parent_action_sequence: command.parent_action_sequence,
                    style: child.style.clone(),
                    recovered: true,
                });
            }
            [] => {}
            _ => return Err(ChildSessionLogicError::AmbiguousRecovery),
        }

        let mut environment = self.environment.clone();
        environment.project_style_root = Some(
            PathBuf::from(&command.workspace)
                .join(".agentmod")
                .join("styles"),
        );
        let style = RuntimeLogic::new(self.data.clone())
            .resolve_style(InspectStyleCommand {
                selector: command.style_selector,
                environment,
            })
            .map_err(ChildSessionLogicError::Style)?
            .binding;
        let prepared = self
            .data
            .prepare(PrepareSessionDataRequest {
                workspace: PathBuf::from(&command.workspace),
            })
            .map_err(ChildSessionLogicError::Registry)?;
        let session_id = prepared.session_id;
        let created = seal_created(&prepared, &style)?;
        let identity = self
            .data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(ChildSessionLogicError::Identity)?;
        let linked = EventEnvelope::seal(
            EventMetadata {
                event_id: identity.event_id,
                scope: EventScope::Session(session_id),
                sequence: Sequence::new(2).map_err(|_| ChildSessionLogicError::Event)?,
                timestamp: identity.timestamp,
                event_type: String::from("child_session.linked"),
                event_version: Version::new(1, 0),
                correlation_id: identity.correlation_id,
                causation_id: CausationId::from_uuid(created.metadata.event_id.into_uuid()),
                parent_graph_node_id: Some(command.parent_graph_node_id.clone()),
                origin: runtime_origin(),
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            RuntimeCommittedEvent::ChildSessionLinked(ChildSessionLinkedEvent {
                parent_session_id: command.parent_session_id,
                parent_action_sequence: command.parent_action_sequence,
                parent_graph_node_id: command.parent_graph_node_id.clone(),
                task_id: command.task_id.clone(),
                revision: command.revision,
                depth: command.depth,
                task: command.task.clone(),
                input_hash: ContentHash::digest(command.task.as_bytes()),
                token_budget: command.token_budget,
            }),
        )
        .map_err(|_| ChildSessionLogicError::Event)?;
        let events = [created, linked]
            .into_iter()
            .map(|event| {
                Ok(BranchEventDataRecord {
                    sequence: event.metadata.sequence.get(),
                    event_id: event.metadata.event_id.to_string(),
                    event_json: serde_json::to_vec(&event)
                        .map_err(|_| ChildSessionLogicError::Event)?,
                })
            })
            .collect::<Result<Vec<_>, ChildSessionLogicError>>()?;
        let selected_style = style.id.clone();
        self.data
            .create_child_session(CreateChildSessionDataRequest {
                sessions_root: command.sessions_root,
                prepared,
                style: selected_style.clone(),
                style_binding_json: serde_json::to_string(&style)
                    .map_err(|_| ChildSessionLogicError::Event)?,
                style_manifest_json: style.configuration_json.clone(),
                compiled_style_json: style.compiled_style_json.clone(),
                parent_session_id: command.parent_session_id,
                parent_action_sequence: command.parent_action_sequence.get(),
                parent_graph_node_id: command.parent_graph_node_id,
                task_id: command.task_id,
                events,
            })
            .map_err(ChildSessionLogicError::Registry)?;
        Ok(ChildSessionResult {
            session_id,
            parent_action_sequence: command.parent_action_sequence,
            style: selected_style,
            recovered: false,
        })
    }
}

fn seal_created(
    prepared: &agentmod_runtime_data::registry::PreparedSessionDataRecord,
    style: &SessionStyleBinding,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, ChildSessionLogicError> {
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
            origin: runtime_origin(),
            schema_version: Version::new(1, 0),
            artifacts: vec![],
            classification: EventClassification::Committed,
        },
        RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
            workspace: prepared.normalized_workspace.to_string_lossy().into_owned(),
            style: style.id.clone(),
            style_binding: Some(Box::new(style.clone())),
        }),
    )
    .map_err(|_| ChildSessionLogicError::Event)
}

fn runtime_origin() -> EventOrigin {
    EventOrigin {
        subsystem: String::from("runtime"),
        plugin: None,
    }
}

fn validate(command: &EnsureChildSessionCommand) -> Result<(), ChildSessionLogicError> {
    if command.sessions_root.as_os_str().is_empty()
        || command.workspace.trim().is_empty()
        || command.style_selector.trim().is_empty()
        || command.parent_graph_node_id.trim().is_empty()
        || command.task_id.trim().is_empty()
        || command.task.trim().is_empty()
        || command.task.len() > 64 * 1024
        || command.depth == 0
        || command.token_budget == 0
    {
        return Err(ChildSessionLogicError::Invalid);
    }
    Ok(())
}

fn child_style_id(selector: &str) -> &str {
    selector
        .split_once('@')
        .map_or(selector, |(style_id, _)| style_id)
}

/// Runtime-managed worker creation or reconciliation failure.
#[derive(Debug, Error)]
pub enum ChildSessionLogicError {
    /// Required child identity or limits are invalid.
    #[error("child session request is invalid")]
    Invalid,
    /// More than one child claims the same parent proposal.
    #[error("child session recovery is ambiguous")]
    AmbiguousRecovery,
    /// Existing child metadata conflicts with the exact request.
    #[error("recovered child session identity does not match the request")]
    RecoveryIdentityMismatch,
    /// Exact child style resolution failed.
    #[error("child style could not be resolved: {0}")]
    Style(SessionStyleLogicError),
    /// Session catalog creation or inspection failed.
    #[error("child session registry failed: {0}")]
    Registry(agentmod_runtime_data::registry::SessionRegistryDataError),
    /// A fresh event identity could not be allocated.
    #[error("child event identity failed: {0}")]
    Identity(agentmod_runtime_data::identity::EventIdentityDataError),
    /// Exact child journal recovery failed.
    #[error("child session persistence failed: {0}")]
    Persistence(crate::persistence::SessionPersistenceLogicError),
    /// A canonical child event could not be sealed.
    #[error("child session event mapping failed")]
    Event,
}
