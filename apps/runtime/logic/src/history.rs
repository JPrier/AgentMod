//! Pure session inspection, point-in-time replay, and atomic branching.

use std::path::PathBuf;

use agentmod_event_model::{
    ArtifactIdentifier, ArtifactReference, EventClassification, EventEnvelope, EventMetadata,
    EventOrigin, EventScope,
};
use agentmod_primitives::{ArtifactId, ContentHash, EventId, Sequence, SessionId, Version};
use agentmod_runtime_data::{
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataError, EventIdentityDataPort},
    journal::{JournalDataError, JournalEventDataPort, ScanEventsDataRequest},
    node_executor::NodeExecutorDataPort,
    registry::{
        BranchArtifactDataRecord, BranchEventDataRecord, BranchMcpBootstrapData,
        CreateBranchDataRequest, PrepareSessionDataRequest, SessionRegistryDataError,
        SessionRegistryDataPort,
    },
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    conversation::{ArtifactEntry, ConversationEntry, ConversationEntryId, ProjectionProvenance},
    node_executor::{RuntimeExecutabilityError, bind_runtime_execution_plan},
    session::{
        ContextProjectionReplacedEvent, ConversationEntryCommittedEvent, RuntimeCommittedEvent,
        SessionBranchedEvent, SessionCreatedEvent, SessionReducerError, SessionState,
        SessionStyleBinding, replay_to,
    },
};

const INLINE_BRANCH_ENTRY_LIMIT: usize = 32;
const INLINE_BRANCH_BYTE_LIMIT: usize = 64 * 1024;
const BRANCH_PROJECTION_ENTRY_LIMIT: usize = 16;
const BRANCH_PROJECTION_BYTE_LIMIT: usize = 64 * 1024;
const BRANCH_ARTIFACT_BYTE_LIMIT: usize = 16 * 1024 * 1024;

/// Point-in-time inspection command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectSessionCommand {
    /// Canonical sessions root.
    pub sessions_root: PathBuf,
    /// Session selected by the endpoint.
    pub session_id: SessionId,
    /// Inclusive replay target; absent means the verified head.
    pub at: Option<Sequence>,
}

/// Pure replay result.
#[derive(Clone, Debug, PartialEq)]
pub struct InspectSessionResult {
    /// Reconstructed state at the requested point.
    pub state: SessionState,
    /// Verified source journal head.
    pub head_sequence: Sequence,
    /// Inclusive sequence represented by `state`.
    pub inspected_sequence: Sequence,
    /// Number of events reduced.
    pub event_count: u64,
}

/// Durable event catch-up command used by reconnecting frontends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeSessionCommand {
    /// Canonical sessions root.
    pub sessions_root: PathBuf,
    /// Session selected by the endpoint.
    pub session_id: SessionId,
    /// Last contiguous canonical sequence already received.
    pub after: Option<Sequence>,
    /// Maximum event records returned in this page.
    pub limit: u32,
}

/// One logic-owned canonical event projection.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventResult {
    /// Canonical event identifier.
    pub event_id: EventId,
    /// Canonical sequence.
    pub sequence: Sequence,
    /// Stable typed event name.
    pub event_type: String,
    /// Typed payload represented without transport ownership.
    pub payload: serde_json::Value,
}

/// Bounded durable catch-up page.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventPage {
    /// Verified journal head at scan time.
    pub head_sequence: Sequence,
    /// Last sequence included, or the caller cursor for an empty page.
    pub last_delivered_sequence: Option<Sequence>,
    /// Whether another immediate catch-up page exists.
    pub has_more: bool,
    /// Strictly ordered canonical event projections.
    pub events: Vec<SessionEventResult>,
}

/// Immutable branch command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionCommand {
    /// Canonical sessions root.
    pub sessions_root: PathBuf,
    /// Parent session.
    pub parent_session_id: SessionId,
    /// Inclusive parent fork point.
    pub at: Sequence,
    /// Optional explicitly resolved style replacement.
    pub style_binding: Option<SessionStyleBinding>,
}

/// Atomic branch result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionResult {
    /// Fresh child session.
    pub session_id: SessionId,
    /// Immutable parent session.
    pub parent_session_id: SessionId,
    /// Inclusive fork point.
    pub fork_sequence: Sequence,
    /// Last event in the newly materialized child.
    pub child_head_sequence: Sequence,
    /// Durable child directory.
    pub session_directory: PathBuf,
}

/// Narrow history business interface.
pub trait SessionHistoryLogicPort {
    /// Reconstructs session state without repeating external side effects.
    ///
    /// # Errors
    ///
    /// Returns [`SessionHistoryLogicError`] for invalid paths, targets, journals,
    /// event mappings, or reducer failures.
    fn inspect_session(
        &self,
        command: InspectSessionCommand,
    ) -> Result<InspectSessionResult, SessionHistoryLogicError>;

    /// Reads one verified, bounded journal page after a reconnect cursor.
    ///
    /// # Errors
    ///
    /// Returns [`SessionHistoryLogicError`] for invalid bounds, journal
    /// corruption, identity mismatch, or event mapping failure.
    fn subscribe_session(
        &self,
        command: SubscribeSessionCommand,
    ) -> Result<SessionEventPage, SessionHistoryLogicError>;

    /// Creates an independently appendable session from point-in-time state.
    ///
    /// # Errors
    ///
    /// Returns [`SessionHistoryLogicError`] if source replay, event remapping,
    /// identity allocation, or atomic persistence fails.
    #[allow(
        clippy::too_many_lines,
        reason = "branching keeps identity, artifact, journal, and atomic persistence decisions visible"
    )]
    fn branch_session(
        &self,
        command: BranchSessionCommand,
    ) -> Result<BranchSessionResult, SessionHistoryLogicError>;
}

impl<D> SessionHistoryLogicPort for super::RuntimeLogic<D>
where
    D: JournalEventDataPort
        + SessionRegistryDataPort
        + EventIdentityDataPort
        + NodeExecutorDataPort,
{
    fn inspect_session(
        &self,
        command: InspectSessionCommand,
    ) -> Result<InspectSessionResult, SessionHistoryLogicError> {
        validate_root(&command.sessions_root)?;
        let scanned = self
            .data
            .scan_events(ScanEventsDataRequest {
                session_directory: command.sessions_root.join(command.session_id.to_string()),
            })
            .map_err(SessionHistoryLogicError::Journal)?;
        let typed = scanned
            .events
            .iter()
            .map(|record| from_data_event(&record.event))
            .collect::<Result<Vec<_>, _>>()?;
        let head_sequence = typed
            .last()
            .map(|event| event.metadata.sequence)
            .ok_or(SessionHistoryLogicError::EmptyJournal)?;
        let target = command.at.unwrap_or(head_sequence);
        if target > head_sequence {
            return Err(SessionHistoryLogicError::SequenceAfterHead {
                requested: target,
                head: head_sequence,
            });
        }
        let state = replay_to(&typed, target).map_err(SessionHistoryLogicError::Reducer)?;
        if state.id != command.session_id {
            return Err(SessionHistoryLogicError::SessionIdentityMismatch);
        }
        Ok(InspectSessionResult {
            state,
            head_sequence,
            inspected_sequence: target,
            event_count: target.get(),
        })
    }

    fn subscribe_session(
        &self,
        command: SubscribeSessionCommand,
    ) -> Result<SessionEventPage, SessionHistoryLogicError> {
        validate_root(&command.sessions_root)?;
        if command.limit == 0 || command.limit > 1_024 {
            return Err(SessionHistoryLogicError::InvalidSubscriptionLimit);
        }
        let scanned = self
            .data
            .scan_events(ScanEventsDataRequest {
                session_directory: command.sessions_root.join(command.session_id.to_string()),
            })
            .map_err(SessionHistoryLogicError::Journal)?;
        let typed = scanned
            .events
            .iter()
            .map(|record| from_data_event(&record.event))
            .collect::<Result<Vec<_>, _>>()?;
        let head_sequence = typed
            .last()
            .map(|event| event.metadata.sequence)
            .ok_or(SessionHistoryLogicError::EmptyJournal)?;
        if let Some(after) = command.after
            && after > head_sequence
        {
            return Err(SessionHistoryLogicError::SequenceAfterHead {
                requested: after,
                head: head_sequence,
            });
        }
        if typed.iter().any(|event| {
            event.metadata.scope != agentmod_event_model::EventScope::Session(command.session_id)
        }) {
            return Err(SessionHistoryLogicError::SessionIdentityMismatch);
        }
        let after = command.after.map_or(0, Sequence::get);
        let limit = usize::try_from(command.limit)
            .map_err(|_| SessionHistoryLogicError::InvalidSubscriptionLimit)?;
        let mut matching = typed
            .into_iter()
            .filter(|event| event.metadata.sequence.get() > after);
        let mut selected = matching.by_ref().take(limit).collect::<Vec<_>>();
        let has_more = matching.next().is_some();
        let events = selected
            .drain(..)
            .map(|event| {
                let payload = serde_json::to_value(event.payload)
                    .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?;
                Ok(SessionEventResult {
                    event_id: event.metadata.event_id,
                    sequence: event.metadata.sequence,
                    event_type: event.metadata.event_type,
                    payload,
                })
            })
            .collect::<Result<Vec<_>, SessionHistoryLogicError>>()?;
        let last_delivered_sequence = events.last().map(|event| event.sequence).or(command.after);
        Ok(SessionEventPage {
            head_sequence,
            last_delivered_sequence,
            has_more,
            events,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "branching keeps identity, artifact, journal, and atomic persistence decisions visible"
    )]
    fn branch_session(
        &self,
        command: BranchSessionCommand,
    ) -> Result<BranchSessionResult, SessionHistoryLogicError> {
        let inspection = self.inspect_session(InspectSessionCommand {
            sessions_root: command.sessions_root.clone(),
            session_id: command.parent_session_id,
            at: Some(command.at),
        })?;
        let inherited_style_binding = inspection.state.style_binding.clone();
        let mcp_bootstrap = branch_mcp_bootstrap(
            command.style_binding.as_ref(),
            inherited_style_binding.as_ref(),
            command.parent_session_id,
        )?;
        let mut style_binding =
            branch_style_binding(command.style_binding, inherited_style_binding)?;
        validate_style_binding(&style_binding)?;
        bind_runtime_execution_plan(&self.data, &mut style_binding)
            .map_err(SessionHistoryLogicError::RuntimeExecutability)?;
        let style = style_binding.id.clone();
        let prepared = self
            .data
            .prepare(PrepareSessionDataRequest {
                workspace: PathBuf::from(&inspection.state.workspace),
            })
            .map_err(SessionHistoryLogicError::Registry)?;
        let child_id = prepared.session_id;
        let mut events = Vec::new();
        let created = EventEnvelope::seal(
            EventMetadata {
                event_id: prepared.event_id,
                scope: EventScope::Session(child_id),
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
                workspace: inspection.state.workspace.clone(),
                style: style.clone(),
                style_binding: Some(Box::new(style_binding.clone())),
            }),
        )
        .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?;
        events.push(to_branch_record(&created)?);

        let branch_payload = RuntimeCommittedEvent::SessionBranched(SessionBranchedEvent {
            parent_session_id: command.parent_session_id,
            fork_sequence: command.at,
        });
        events.push(self.seal_branch_event(child_id, Sequence::new(2)?, branch_payload)?);

        let mut artifacts = Vec::new();
        let history = inspection.state.conversation.history();
        let inline_bytes = serde_json::to_vec(history)
            .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?;
        if history.len() <= INLINE_BRANCH_ENTRY_LIMIT
            && inline_bytes.len() <= INLINE_BRANCH_BYTE_LIMIT
        {
            append_inline_branch_context(self, child_id, &inspection.state, &mut events)?;
        } else {
            let snapshot = serde_json::to_vec(&BranchContextSnapshot {
                schema_version: 1,
                parent_session_id: command.parent_session_id,
                fork_sequence: command.at,
                history,
                provider_projection: inspection.state.conversation.provider_projection(),
                projection_provenance: inspection.state.conversation.projection_provenance(),
            })
            .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?;
            if snapshot.len() > BRANCH_ARTIFACT_BYTE_LIMIT {
                return Err(SessionHistoryLogicError::BranchContextTooLarge);
            }
            let identity = self
                .data
                .allocate_event_identity(AllocateEventIdentityDataRequest)
                .map_err(SessionHistoryLogicError::Identity)?;
            let artifact_id = ArtifactId::from_uuid(identity.event_id.into_uuid());
            let content_hash = ContentHash::digest(&snapshot);
            append_artifact_branch_context(
                self,
                child_id,
                command.at,
                artifact_id,
                content_hash,
                inspection.state.conversation.provider_projection(),
                &mut events,
            )?;
            let creation_event = events
                .get(2)
                .ok_or(SessionHistoryLogicError::EventMapping(String::from(
                    "branch artifact reference event is missing",
                )))?
                .event_id
                .clone();
            artifacts.push(BranchArtifactDataRecord {
                artifact_id: artifact_id.to_string(),
                content_hash: content_hash.to_hex(),
                mime_type: String::from("application/vnd.agentmod.branch-context+json"),
                creation_event,
                bytes: snapshot,
            });
        }
        let child_head_sequence = Sequence::new(
            u64::try_from(events.len()).map_err(|_| SessionHistoryLogicError::SequenceOverflow)?,
        )?;
        let created = self
            .data
            .create_branch(CreateBranchDataRequest {
                sessions_root: command.sessions_root,
                prepared,
                style,
                style_binding_json: serde_json::to_string(&style_binding)
                    .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?,
                style_manifest_json: style_binding.configuration_json.clone(),
                compiled_style_json: style_binding.compiled_style_json.clone(),
                parent_session_id: command.parent_session_id,
                fork_sequence: command.at.get(),
                mcp_bootstrap,
                events,
                artifacts,
            })
            .map_err(SessionHistoryLogicError::Registry)?;
        Ok(BranchSessionResult {
            session_id: child_id,
            parent_session_id: command.parent_session_id,
            fork_sequence: command.at,
            child_head_sequence,
            session_directory: created.session_directory,
        })
    }
}

impl<D> super::RuntimeLogic<D>
where
    D: EventIdentityDataPort,
{
    fn seal_branch_event(
        &self,
        session_id: SessionId,
        sequence: Sequence,
        payload: RuntimeCommittedEvent,
    ) -> Result<BranchEventDataRecord, SessionHistoryLogicError> {
        self.seal_branch_event_with_artifacts(session_id, sequence, payload, Vec::new())
    }

    fn seal_branch_event_with_artifacts(
        &self,
        session_id: SessionId,
        sequence: Sequence,
        payload: RuntimeCommittedEvent,
        artifacts: Vec<ArtifactReference>,
    ) -> Result<BranchEventDataRecord, SessionHistoryLogicError> {
        let identity = self
            .data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(SessionHistoryLogicError::Identity)?;
        let event_type = payload.event_type().to_owned();
        let event = EventEnvelope::seal(
            EventMetadata {
                event_id: identity.event_id,
                scope: EventScope::Session(session_id),
                sequence,
                timestamp: identity.timestamp,
                event_type,
                event_version: Version::new(1, 0),
                correlation_id: identity.correlation_id,
                causation_id: identity.causation_id,
                parent_graph_node_id: None,
                origin: runtime_origin(),
                schema_version: Version::new(1, 0),
                artifacts,
                classification: EventClassification::Committed,
            },
            payload,
        )
        .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?;
        to_branch_record(&event)
    }
}

#[derive(Serialize)]
struct BranchContextSnapshot<'a> {
    schema_version: u32,
    parent_session_id: SessionId,
    fork_sequence: Sequence,
    history: &'a [ConversationEntry],
    provider_projection: &'a [ConversationEntry],
    projection_provenance: Option<&'a ProjectionProvenance>,
}

fn append_inline_branch_context<D>(
    logic: &super::RuntimeLogic<D>,
    child_id: SessionId,
    state: &SessionState,
    events: &mut Vec<BranchEventDataRecord>,
) -> Result<(), SessionHistoryLogicError>
where
    D: EventIdentityDataPort,
{
    for entry in state.conversation.history() {
        let sequence = next_sequence(events.len())?;
        events.push(logic.seal_branch_event(
            child_id,
            sequence,
            RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
                entry: entry.clone(),
            }),
        )?);
    }
    if let Some(source) = state.conversation.projection_provenance() {
        let sequence = next_sequence(events.len())?;
        let provenance = ProjectionProvenance {
            committed_at: sequence,
            ..source.clone()
        };
        events.push(logic.seal_branch_event(
            child_id,
            sequence,
            RuntimeCommittedEvent::ContextProjectionReplaced(ContextProjectionReplacedEvent {
                replacement: state.conversation.provider_projection().to_vec(),
                provenance,
                context_phase: None,
            }),
        )?);
    }
    Ok(())
}

fn append_artifact_branch_context<D>(
    logic: &super::RuntimeLogic<D>,
    child_id: SessionId,
    fork_sequence: Sequence,
    artifact_id: ArtifactId,
    content_hash: ContentHash,
    source_projection: &[ConversationEntry],
    events: &mut Vec<BranchEventDataRecord>,
) -> Result<(), SessionHistoryLogicError>
where
    D: EventIdentityDataPort,
{
    let artifact_sequence = next_sequence(events.len())?;
    let artifact_entry = ConversationEntry::ArtifactReference(ArtifactEntry {
        id: ConversationEntryId(format!("branch-context:{artifact_id}")),
        artifact_id,
        artifact_reference: None,
        content_hash,
        mime_type: String::from("application/vnd.agentmod.branch-context+json"),
        label: String::from("complete parent context at branch point"),
        source_sequence: artifact_sequence,
    });
    events.push(logic.seal_branch_event_with_artifacts(
        child_id,
        artifact_sequence,
        RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
            entry: artifact_entry.clone(),
        }),
        vec![ArtifactReference {
            id: ArtifactIdentifier::from(artifact_id),
            content_hash,
        }],
    )?);
    let retained = bounded_projection_tail(source_projection)?;
    for entry in &retained {
        let sequence = next_sequence(events.len())?;
        events.push(logic.seal_branch_event(
            child_id,
            sequence,
            RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
                entry: entry.clone(),
            }),
        )?);
    }
    let sequence = next_sequence(events.len())?;
    let mut replacement = Vec::with_capacity(retained.len() + 1);
    replacement.push(artifact_entry);
    replacement.extend(retained);
    events.push(logic.seal_branch_event_with_artifacts(
        child_id,
        sequence,
        RuntimeCommittedEvent::ContextProjectionReplaced(ContextProjectionReplacedEvent {
            replacement,
            provenance: ProjectionProvenance {
                projection_id: format!("branch-artifact:{artifact_id}"),
                source_range: Some((Sequence::FIRST, fork_sequence)),
                method: String::from("branch_artifact_handoff"),
                committed_at: sequence,
                artifact_id: Some(artifact_id),
            },
            context_phase: None,
        }),
        vec![ArtifactReference {
            id: ArtifactIdentifier::from(artifact_id),
            content_hash,
        }],
    )?);
    Ok(())
}

fn bounded_projection_tail(
    source: &[ConversationEntry],
) -> Result<Vec<ConversationEntry>, SessionHistoryLogicError> {
    let mut bytes = 0_usize;
    let mut retained = Vec::new();
    for entry in source.iter().rev() {
        if retained.len() == BRANCH_PROJECTION_ENTRY_LIMIT {
            break;
        }
        let entry_bytes = serde_json::to_vec(entry)
            .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?
            .len();
        if bytes
            .checked_add(entry_bytes)
            .is_none_or(|total| total > BRANCH_PROJECTION_BYTE_LIMIT)
        {
            break;
        }
        bytes += entry_bytes;
        retained.push(entry.clone());
    }
    retained.reverse();
    Ok(retained)
}

fn from_data_event(
    event: &EventEnvelope<serde_json::Value>,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, SessionHistoryLogicError> {
    let payload = serde_json::from_value(event.payload.clone())
        .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?;
    let mapped = EventEnvelope::seal(event.metadata.clone(), payload)
        .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?;
    if mapped.integrity_checksum != event.integrity_checksum {
        return Err(SessionHistoryLogicError::MappingChangedChecksum);
    }
    Ok(mapped)
}

fn to_branch_record(
    event: &EventEnvelope<RuntimeCommittedEvent>,
) -> Result<BranchEventDataRecord, SessionHistoryLogicError> {
    Ok(BranchEventDataRecord {
        sequence: event.metadata.sequence.get(),
        event_id: event.metadata.event_id.to_string(),
        event_json: serde_json::to_vec(event)
            .map_err(|error| SessionHistoryLogicError::EventMapping(error.to_string()))?,
    })
}

fn next_sequence(existing_events: usize) -> Result<Sequence, SessionHistoryLogicError> {
    let value = u64::try_from(existing_events)
        .map_err(|_| SessionHistoryLogicError::SequenceOverflow)?
        .checked_add(1)
        .ok_or(SessionHistoryLogicError::SequenceOverflow)?;
    Sequence::new(value).map_err(|_| SessionHistoryLogicError::SequenceOverflow)
}

fn validate_root(root: &std::path::Path) -> Result<(), SessionHistoryLogicError> {
    if root.as_os_str().is_empty() {
        Err(SessionHistoryLogicError::InvalidSessionsRoot)
    } else {
        Ok(())
    }
}

fn validate_style_binding(style: &SessionStyleBinding) -> Result<(), SessionHistoryLogicError> {
    if style.id.is_empty()
        || style.id.len() > 128
        || !style
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || style.content_hash != ContentHash::digest(style.configuration_json.as_bytes())
        || style.compiled_style_hash != ContentHash::digest(style.compiled_style_json.as_bytes())
    {
        Err(SessionHistoryLogicError::InvalidStyle)
    } else {
        Ok(())
    }
}

/// Selects the immutable style binding for a branch without accidentally
/// migrating a legacy parent binding.  Supplying a replacement binding is the
/// explicit branch-with-recompiled-style migration path; inheriting requires
/// the parent's exact persisted node-execution contract.
fn branch_style_binding(
    replacement: Option<SessionStyleBinding>,
    inherited: Option<SessionStyleBinding>,
) -> Result<SessionStyleBinding, SessionHistoryLogicError> {
    if let Some(replacement) = replacement {
        return Ok(replacement);
    }
    let inherited = inherited.ok_or(SessionHistoryLogicError::MissingStyleBinding)?;
    if inherited.execution_plan.is_none() || inherited.execution_plan_hash.is_none() {
        return Err(SessionHistoryLogicError::ExecutionPlanMigrationRequired);
    }
    Ok(inherited)
}

fn branch_mcp_bootstrap(
    replacement: Option<&SessionStyleBinding>,
    inherited: Option<&SessionStyleBinding>,
    parent_session_id: SessionId,
) -> Result<BranchMcpBootstrapData, SessionHistoryLogicError> {
    let selected = replacement
        .or(inherited)
        .ok_or(SessionHistoryLogicError::MissingStyleBinding)?;
    let selected_has_bootstrap = selected.mcp.configuration_reference.is_some();
    if !selected_has_bootstrap {
        if selected.mcp.servers.is_empty() {
            return Ok(BranchMcpBootstrapData::None);
        }
        return Err(SessionHistoryLogicError::McpBootstrapMigrationRequired);
    }
    let inherited = inherited.ok_or(SessionHistoryLogicError::McpBootstrapMigrationRequired)?;
    if replacement.is_some() && selected.mcp != inherited.mcp {
        return Err(SessionHistoryLogicError::McpBootstrapMigrationRequired);
    }
    if inherited.mcp.configuration_reference.is_none() || inherited.mcp != selected.mcp {
        return Err(SessionHistoryLogicError::McpBootstrapMigrationRequired);
    }
    Ok(BranchMcpBootstrapData::InheritExact {
        source_session_id: parent_session_id,
    })
}

#[cfg(test)]
mod tests {
    use agentmod_runtime_data::node_executor::RuntimeNodeExecutorData;
    use agentmod_session_style_sdk::BuiltInStyle;

    use super::{
        BranchMcpBootstrapData, SessionHistoryLogicError, bind_runtime_execution_plan,
        branch_mcp_bootstrap, branch_style_binding,
    };

    #[test]
    fn inherited_planless_binding_requires_explicit_branch_migration() {
        let mut inherited = crate::style_executor::tests::binding(BuiltInStyle::PersistentChat);
        inherited.execution_plan = None;
        inherited.execution_plan_hash = None;

        assert!(matches!(
            branch_style_binding(None, Some(inherited)),
            Err(SessionHistoryLogicError::ExecutionPlanMigrationRequired)
        ));
    }

    #[test]
    fn explicit_planless_replacement_compiles_and_binds_as_authorized_branch_migration() {
        let mut replacement = crate::style_executor::tests::binding(BuiltInStyle::PersistentChat);
        replacement.execution_plan = None;
        replacement.execution_plan_hash = None;

        let mut selected =
            branch_style_binding(Some(replacement), None).expect("replacement binding");
        let registry = RuntimeNodeExecutorData::native().expect("native registry");
        bind_runtime_execution_plan(&registry, &mut selected).expect("compile replacement plan");
        assert!(selected.execution_plan.is_some());
        assert!(selected.execution_plan_hash.is_some());
    }

    #[test]
    fn branch_mcp_bootstrap_requires_exact_inheritance_or_explicit_none() {
        let parent_session_id = agentmod_primitives::SessionId::from_uuid(uuid::Uuid::from_u128(7));
        let empty = crate::style_executor::tests::binding(BuiltInStyle::PersistentChat);
        assert_eq!(
            branch_mcp_bootstrap(None, Some(&empty), parent_session_id).expect("empty binding"),
            BranchMcpBootstrapData::None
        );
        assert_eq!(
            branch_mcp_bootstrap(Some(&empty), None, parent_session_id)
                .expect("explicit non-MCP migration"),
            BranchMcpBootstrapData::None
        );

        let mut inherited = empty.clone();
        inherited.mcp.configuration_reference = Some(String::from("session-mcp:blake3:exact"));
        assert_eq!(
            branch_mcp_bootstrap(None, Some(&inherited), parent_session_id)
                .expect("exact inheritance"),
            BranchMcpBootstrapData::InheritExact {
                source_session_id: parent_session_id,
            }
        );

        let mut substituted = inherited.clone();
        substituted.mcp.declaration_hash = agentmod_primitives::ContentHash::digest(b"substituted");
        assert!(matches!(
            branch_mcp_bootstrap(Some(&substituted), Some(&inherited), parent_session_id),
            Err(SessionHistoryLogicError::McpBootstrapMigrationRequired)
        ));
        assert_eq!(
            branch_mcp_bootstrap(Some(&empty), Some(&inherited), parent_session_id)
                .expect("explicit non-inheritance"),
            BranchMcpBootstrapData::None
        );
    }
}

fn runtime_origin() -> EventOrigin {
    EventOrigin {
        subsystem: String::from("runtime"),
        plugin: None,
    }
}

/// History business failure.
#[derive(Debug, Error)]
pub enum SessionHistoryLogicError {
    /// Configured root is empty.
    #[error("sessions root is invalid")]
    InvalidSessionsRoot,
    /// Requested style is unsafe.
    #[error("session style identifier is invalid")]
    InvalidStyle,
    /// The branch style is compiled but cannot execute in this runtime.
    #[error("branch style is not runtime-executable: {0}")]
    RuntimeExecutability(RuntimeExecutabilityError),
    /// Session predates immutable style binding and needs explicit migration.
    #[error("session has no immutable style binding; select a replacement style explicitly")]
    MissingStyleBinding,
    /// An inherited legacy binding has no exact persisted execution contract.
    #[error(
        "inherited session style has no immutable node-execution plan; select a replacement style explicitly to migrate the branch"
    )]
    ExecutionPlanMigrationRequired,
    /// A branch requested an MCP activation that cannot be copied exactly.
    #[error("branch MCP bootstrap requires an explicit compatible migration")]
    McpBootstrapMigrationRequired,
    /// Subscription page bound is zero or excessive.
    #[error("session subscription limit is invalid")]
    InvalidSubscriptionLimit,
    /// Source journal is empty.
    #[error("session journal is empty")]
    EmptyJournal,
    /// Requested point is after the verified journal head.
    #[error("requested sequence {requested:?} is after journal head {head:?}")]
    SequenceAfterHead {
        /// Requested sequence.
        requested: Sequence,
        /// Verified head.
        head: Sequence,
    },
    /// Journal identity and endpoint selection differed.
    #[error("session identity does not match the selected journal")]
    SessionIdentityMismatch,
    /// Journal data failed.
    #[error("session journal data failed: {0}")]
    Journal(JournalDataError),
    /// Session registry data failed.
    #[error("session registry data failed: {0}")]
    Registry(SessionRegistryDataError),
    /// Pure reducer rejected the source.
    #[error("session replay failed: {0}")]
    Reducer(SessionReducerError),
    /// External identity source failed.
    #[error("event identity allocation failed: {0}")]
    Identity(EventIdentityDataError),
    /// Typed event mapping failed.
    #[error("event mapping failed: {0}")]
    EventMapping(String),
    /// Value mapping changed a sealed envelope checksum.
    #[error("event mapping changed canonical checksum")]
    MappingChangedChecksum,
    /// Sequence arithmetic overflowed.
    #[error("session sequence overflow")]
    SequenceOverflow,
    /// Full context exceeds the hard artifact bound.
    #[error("branch context exceeds the artifact size limit")]
    BranchContextTooLarge,
}

impl From<agentmod_primitives::PrimitiveError> for SessionHistoryLogicError {
    fn from(_: agentmod_primitives::PrimitiveError) -> Self {
        Self::SequenceOverflow
    }
}
