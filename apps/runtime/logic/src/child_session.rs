//! Runtime-managed child-session creation and orphan recovery.
//!
//! A worker is a fresh session with a typed parent/task link. It is not a
//! history branch and does not fabricate a user conversation entry.

use std::{collections::BTreeSet, path::PathBuf};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{CausationId, ContentHash, Sequence, SessionId, Version};
use agentmod_runtime_data::{
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
    node_executor::NodeExecutorDataPort,
    registry::{
        BranchEventDataRecord, BranchMcpBootstrapData, CreateChildSessionDataRequest,
        ListSessionsDataRequest, PrepareSessionDataRequest, SessionRegistryDataPort,
    },
    style::SessionStyleDataPort,
    workspace::WorkspaceLeaseDataPort,
};
use agentmod_session_style_sdk::ChildMemoryAccess;
use thiserror::Error;

use crate::{
    RuntimeLogic,
    node_executor::{RuntimeExecutabilityError, bind_runtime_execution_plan},
    persistence::{LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort},
    session::{
        ChildSessionLinkedEvent, ChildWorkspaceLeaseBoundEvent, RuntimeCommittedEvent,
        SessionCreatedEvent, SessionMcpBinding, SessionStyleBinding,
    },
    style::{
        ChildStyleMemoryAccess, InspectStyleCommand, SelectChildStyleCommand,
        SessionStyleLogicError, SessionStyleLogicPort, StyleEnvironment,
    },
    workspace::{
        DEFAULT_MAXIMUM_WORKSPACE_BYTES, DEFAULT_MAXIMUM_WORKSPACE_DEPTH,
        DEFAULT_MAXIMUM_WORKSPACE_FILES, EnsureWorkspaceLeaseCommand, WorkspaceLeaseContract,
        WorkspaceLeaseLogic, WorkspaceLeaseLogicError, WorkspaceLeaseMode, WorkspaceLeaseOwner,
        default_workspace_exclusions,
    },
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
    /// Exact parent provider inherited by the child when enabled.
    pub inherited_provider: Option<String>,
    /// Exact parent model inherited by the child when enabled.
    pub inherited_model: Option<String>,
    /// Exact sanitized parent MCP binding inherited by the child, when enabled.
    pub inherited_mcp: Option<SessionMcpBinding>,
    /// Exact immutable artifact references delegated by the parent graph.
    pub artifact_references: BTreeSet<String>,
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
    /// Maximum provider context/token contribution.
    pub context_budget_tokens: u64,
    /// Tool groups retained from the selected child style.
    pub tool_groups: Vec<String>,
    /// Style-selected child memory access.
    pub memory_access: ChildMemoryAccess,
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
    /// Exact immutable workspace lease used by the child.
    pub workspace_lease: WorkspaceLeaseContract,
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
    maximum_cost_budget_micros: Option<u64>,
    workspace_mode: WorkspaceLeaseMode,
}

impl<D> RuntimeChildSessionLogic<D> {
    /// Constructs child coordination with an explicit style environment.
    #[must_use]
    pub const fn new(data: D, environment: StyleEnvironment) -> Self {
        Self {
            data,
            environment,
            maximum_cost_budget_micros: None,
            workspace_mode: WorkspaceLeaseMode::SharedReadOnly,
        }
    }

    /// Applies an additional hard child cost ceiling for one generic graph
    /// dispatch.
    #[must_use]
    pub const fn with_maximum_cost_budget_micros(mut self, maximum: u64) -> Self {
        self.maximum_cost_budget_micros = Some(maximum);
        self
    }

    /// Selects the exact immutable workspace mode for one generic child
    /// dispatch. Frozen callers retain the safe shared-read-only default.
    #[must_use]
    pub fn with_workspace_mode(mut self, mode: WorkspaceLeaseMode) -> Self {
        self.workspace_mode = mode;
        self
    }
}

impl<D> ChildSessionLogicPort for RuntimeChildSessionLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + NodeExecutorDataPort
        + SessionRegistryDataPort
        + SessionStyleDataPort
        + WorkspaceLeaseDataPort,
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
        if self.maximum_cost_budget_micros == Some(0) {
            return Err(ChildSessionLogicError::Invalid);
        }
        validate_workspace_tool_groups(&self.workspace_mode, &command.tool_groups)?;
        let workspace_lease = WorkspaceLeaseLogic::new(self.data.clone())
            .ensure_workspace_lease(EnsureWorkspaceLeaseCommand {
                sessions_root: command.sessions_root.clone(),
                source_workspace: PathBuf::from(&command.workspace),
                owner: WorkspaceLeaseOwner {
                    parent_session_id: command.parent_session_id,
                    parent_action_sequence: command.parent_action_sequence,
                    parent_graph_node_id: command.parent_graph_node_id.clone(),
                    task_id: command.task_id.clone(),
                },
                mode: self.workspace_mode.clone(),
                maximum_files: DEFAULT_MAXIMUM_WORKSPACE_FILES,
                maximum_bytes: DEFAULT_MAXIMUM_WORKSPACE_BYTES,
                maximum_depth: DEFAULT_MAXIMUM_WORKSPACE_DEPTH,
                excluded_names: default_workspace_exclusions(),
            })
            .map_err(ChildSessionLogicError::Workspace)?;
        let mut environment = self.environment.clone();
        environment.project_style_root = Some(
            workspace_lease
                .effective_root
                .join(".agentmod")
                .join("styles"),
        );
        let style_logic = RuntimeLogic::new(self.data.clone());
        let resolved_style = style_logic
            .resolve_style(InspectStyleCommand {
                selector: command.style_selector.clone(),
                environment: environment.clone(),
            })
            .map_err(ChildSessionLogicError::Style)?
            .binding;
        let maximum_tokens = resolved_style
            .budgets
            .max_tokens
            .min(command.token_budget)
            .min(command.context_budget_tokens);
        let maximum_cost_micros = self
            .maximum_cost_budget_micros
            .map(|maximum| resolved_style.budgets.max_cost_micros.min(maximum));
        // An empty child tool grant is an execution-time deny-all projection,
        // not permission to make the immutable child graph uncompilable. Keep
        // the selected graph descriptor intact and narrow the persisted
        // binding projection below; tool authorization consumes that narrowed
        // projection and therefore still fails closed.
        let selected_tool_groups = if command.tool_groups.is_empty() {
            resolved_style.tool_groups.iter().cloned().collect()
        } else {
            command.tool_groups.iter().cloned().collect()
        };
        let mut expected_style = style_logic
            .select_child_style(SelectChildStyleCommand {
                binding: resolved_style,
                tool_groups: selected_tool_groups,
                memory_access: child_style_memory_access(command.memory_access),
                inherited_provider: command.inherited_provider.clone(),
                max_tokens: Some(maximum_tokens),
                max_cost_micros: maximum_cost_micros,
                environment,
            })
            .map_err(ChildSessionLogicError::Style)?
            .binding;
        restrict_child_binding(&mut expected_style, &command)?;
        bind_runtime_execution_plan(&self.data, &mut expected_style)
            .map_err(ChildSessionLogicError::RuntimeExecutability)?;
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
                if child.style != expected_style.id
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
                    || origin.inherited_provider != command.inherited_provider
                    || origin.inherited_model != command.inherited_model
                    || origin.inherited_mcp != command.inherited_mcp
                    || origin.artifact_references != command.artifact_references
                {
                    return Err(ChildSessionLogicError::RecoveryIdentityMismatch);
                }
                validate_child_binding(&loaded.state, &command)?;
                if loaded.state.style_binding.as_ref() != Some(&expected_style) {
                    return Err(ChildSessionLogicError::RecoveryIdentityMismatch);
                }
                if std::path::Path::new(&loaded.state.workspace)
                    != workspace_lease.effective_root.as_path()
                {
                    return Err(ChildSessionLogicError::RecoveryIdentityMismatch);
                }
                WorkspaceLeaseLogic::new(self.data.clone())
                    .bind_session(&command.sessions_root, child.id, &workspace_lease)
                    .map_err(ChildSessionLogicError::Workspace)?;
                return Ok(ChildSessionResult {
                    session_id: child.id,
                    parent_action_sequence: command.parent_action_sequence,
                    style: child.style.clone(),
                    recovered: true,
                    workspace_lease,
                });
            }
            [] => {}
            _ => return Err(ChildSessionLogicError::AmbiguousRecovery),
        }

        let style = expected_style;
        let prepared = self
            .data
            .prepare(PrepareSessionDataRequest {
                workspace: workspace_lease.effective_root.clone(),
            })
            .map_err(ChildSessionLogicError::Registry)?;
        let session_id = prepared.session_id;
        WorkspaceLeaseLogic::new(self.data.clone())
            .bind_session(&command.sessions_root, session_id, &workspace_lease)
            .map_err(ChildSessionLogicError::Workspace)?;
        let created = seal_created(&prepared, &style)?;
        let linked_identity = self
            .data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(ChildSessionLogicError::Identity)?;
        let linked = EventEnvelope::seal(
            EventMetadata {
                event_id: linked_identity.event_id,
                scope: EventScope::Session(session_id),
                sequence: Sequence::new(2).map_err(|_| ChildSessionLogicError::Event)?,
                timestamp: linked_identity.timestamp,
                event_type: String::from("child_session.linked"),
                event_version: Version::new(1, 0),
                correlation_id: linked_identity.correlation_id,
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
                inherited_provider: command.inherited_provider.clone(),
                inherited_model: command.inherited_model.clone(),
                inherited_mcp: command.inherited_mcp.clone(),
                artifact_references: command.artifact_references.clone(),
            }),
        )
        .map_err(|_| ChildSessionLogicError::Event)?;
        let lease_identity = self
            .data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(ChildSessionLogicError::Identity)?;
        let workspace_bound = EventEnvelope::seal(
            EventMetadata {
                event_id: lease_identity.event_id,
                scope: EventScope::Session(session_id),
                sequence: Sequence::new(3).map_err(|_| ChildSessionLogicError::Event)?,
                timestamp: lease_identity.timestamp,
                event_type: String::from("child_session.workspace_lease_bound"),
                event_version: Version::new(1, 0),
                correlation_id: lease_identity.correlation_id,
                causation_id: CausationId::from_uuid(linked.metadata.event_id.into_uuid()),
                parent_graph_node_id: Some(command.parent_graph_node_id.clone()),
                origin: runtime_origin(),
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            RuntimeCommittedEvent::ChildWorkspaceLeaseBound(ChildWorkspaceLeaseBoundEvent {
                lease: workspace_lease.clone(),
            }),
        )
        .map_err(|_| ChildSessionLogicError::Event)?;
        let events = [created, linked, workspace_bound]
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
                mcp_bootstrap: if command.inherited_mcp.is_some() {
                    BranchMcpBootstrapData::InheritExact {
                        source_session_id: command.parent_session_id,
                    }
                } else {
                    BranchMcpBootstrapData::None
                },
                events,
            })
            .map_err(ChildSessionLogicError::Registry)?;
        Ok(ChildSessionResult {
            session_id,
            parent_action_sequence: command.parent_action_sequence,
            style: selected_style,
            recovered: false,
            workspace_lease,
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
        || command.context_budget_tokens == 0
        || command.context_budget_tokens > command.token_budget
        || command
            .inherited_provider
            .as_deref()
            .is_some_and(invalid_child_execution_default)
        || command
            .inherited_model
            .as_deref()
            .is_some_and(invalid_child_execution_default)
        || command.inherited_mcp.as_ref().is_some_and(|binding| {
            binding.servers.is_empty()
                || binding
                    .configuration_reference
                    .as_deref()
                    .is_none_or(invalid_child_execution_default)
                || !command.tool_groups.iter().any(|group| group == "mcp")
        })
        || command.artifact_references.len() > 256
        || command
            .artifact_references
            .iter()
            .any(|reference| invalid_child_execution_default(reference))
    {
        return Err(ChildSessionLogicError::Invalid);
    }
    Ok(())
}

fn invalid_child_execution_default(value: &str) -> bool {
    value.trim().is_empty() || value.len() > 1024 || value.chars().any(char::is_control)
}

fn validate_workspace_tool_groups(
    mode: &WorkspaceLeaseMode,
    tool_groups: &[String],
) -> Result<(), ChildSessionLogicError> {
    if *mode == WorkspaceLeaseMode::SharedReadOnly
        && tool_groups
            .iter()
            .any(|group| workspace_write_capable_group(group))
    {
        Err(ChildSessionLogicError::WorkspaceWriteCapability)
    } else {
        Ok(())
    }
}

fn workspace_write_capable_group(group: &str) -> bool {
    matches!(group, "filesystem" | "git" | "process")
        || group.starts_with("filesystem.write")
        || group.starts_with("git.write")
        || group.starts_with("process.")
}

fn restrict_child_binding(
    binding: &mut SessionStyleBinding,
    command: &EnsureChildSessionCommand,
) -> Result<(), ChildSessionLogicError> {
    binding
        .tool_groups
        .retain(|group| command.tool_groups.contains(group));
    binding.budgets.max_tokens = binding
        .budgets
        .max_tokens
        .min(command.token_budget)
        .min(command.context_budget_tokens);
    binding.mcp = command.inherited_mcp.clone().unwrap_or_default();
    match command.memory_access {
        ChildMemoryAccess::None => {
            binding.memory.provider = String::from("none");
            binding.memory.scopes.clear();
            binding.memory.retrieval_timing = String::from("never");
            binding.memory.write_policy = String::from("never");
            binding.memory.injection_location = String::from("none");
        }
        ChildMemoryAccess::ReadOnly => {
            binding.memory.write_policy = String::from("never");
        }
        ChildMemoryAccess::ReadWrite => {}
    }
    validate_binding_selection(binding, command)
}

const fn child_style_memory_access(value: ChildMemoryAccess) -> ChildStyleMemoryAccess {
    match value {
        ChildMemoryAccess::None => ChildStyleMemoryAccess::None,
        ChildMemoryAccess::ReadOnly => ChildStyleMemoryAccess::ReadOnly,
        ChildMemoryAccess::ReadWrite => ChildStyleMemoryAccess::ReadWrite,
    }
}

fn validate_child_binding(
    state: &crate::session::SessionState,
    command: &EnsureChildSessionCommand,
) -> Result<(), ChildSessionLogicError> {
    let binding = state
        .style_binding
        .as_ref()
        .ok_or(ChildSessionLogicError::RecoveryIdentityMismatch)?;
    validate_binding_selection(binding, command)
}

fn validate_binding_selection(
    binding: &SessionStyleBinding,
    command: &EnsureChildSessionCommand,
) -> Result<(), ChildSessionLogicError> {
    let tools_valid = binding
        .tool_groups
        .iter()
        .all(|group| command.tool_groups.contains(group));
    let memory_valid = match command.memory_access {
        ChildMemoryAccess::None => {
            binding.memory.provider == "none"
                && binding.memory.retrieval_timing == "never"
                && binding.memory.write_policy == "never"
        }
        ChildMemoryAccess::ReadOnly => binding.memory.write_policy == "never",
        ChildMemoryAccess::ReadWrite => true,
    };
    if !tools_valid
        || !memory_valid
        || binding.mcp != command.inherited_mcp.clone().unwrap_or_default()
        || command
            .inherited_provider
            .as_deref()
            .is_some_and(|provider| {
                crate::style::child_binding_inherited_provider(binding).as_deref() != Some(provider)
            })
        || binding.budgets.max_tokens > command.token_budget
        || binding.budgets.max_tokens > command.context_budget_tokens
    {
        return Err(ChildSessionLogicError::RecoveryIdentityMismatch);
    }
    Ok(())
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
    /// The selected child graph cannot execute in this runtime.
    #[error("child style is not runtime-executable: {0}")]
    RuntimeExecutability(RuntimeExecutabilityError),
    /// Workspace lease materialization or recovery failed.
    #[error("child workspace lease failed: {0}")]
    Workspace(WorkspaceLeaseLogicError),
    /// Shared-read-only child requested a workspace write-capable tool group.
    #[error("shared-read-only child requested a workspace write capability")]
    WorkspaceWriteCapability,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agentmod_primitives::{ContentHash, SessionId};
    use uuid::Uuid;

    use super::*;
    use crate::session::{
        SessionCompactionConfiguration, SessionMcpServerBinding, SessionMcpTransportBinding,
        SessionMemoryConfiguration, SessionPermissionDefaults, SessionStyleBudgets,
        SessionStyleSource,
    };

    fn command(memory_access: ChildMemoryAccess) -> EnsureChildSessionCommand {
        EnsureChildSessionCommand {
            sessions_root: PathBuf::from("sessions"),
            parent_session_id: SessionId::from_uuid(Uuid::from_u128(1)),
            parent_action_sequence: Sequence::new(2).expect("sequence"),
            parent_graph_node_id: String::from("spawn"),
            workspace: String::from("workspace"),
            style_selector: String::from("child@1.0.0"),
            inherited_provider: None,
            inherited_model: None,
            inherited_mcp: None,
            artifact_references: BTreeSet::new(),
            task_id: String::from("task-1"),
            revision: 0,
            depth: 1,
            task: String::from("execute task"),
            token_budget: 500,
            context_budget_tokens: 300,
            tool_groups: vec![String::from("filesystem.read")],
            memory_access,
        }
    }

    fn binding() -> SessionStyleBinding {
        let hash = ContentHash::digest(b"fixture");
        SessionStyleBinding {
            id: String::from("child"),
            version: String::from("1.0.0"),
            content_hash: hash,
            compiled_cache_key: hash,
            compiled_style_hash: hash,
            source: SessionStyleSource::BuiltIn,
            source_locator: String::from("built-in:child"),
            plugin_set_hash: hash,
            capability_set_hash: hash,
            runtime_api_version: String::from("1.0.0"),
            configuration_json: String::from("{}"),
            compiled_style_json: String::from("{}"),
            execution_plan: None,
            execution_plan_hash: None,
            mcp: crate::session::SessionMcpBinding::default(),
            memory: SessionMemoryConfiguration {
                provider: String::from("file"),
                plugin: None,
                scopes: vec![String::from("session")],
                retrieval_timing: String::from("before_model_request"),
                query_json: String::from("{}"),
                max_items: 10,
                max_injected_bytes: 1_024,
                write_policy: String::from("turn_completion"),
                injection_location: String::from("before_current_input"),
            },
            compaction: SessionCompactionConfiguration {
                strategy: String::from("none"),
                plugin: None,
                trigger_tokens: None,
                reserved_context_tokens: 0,
                max_provider_projection_tokens: 1_000,
                preserve_unresolved_tasks: true,
                preserve_active_processes: true,
                preservation_requirements: Vec::new(),
            },
            tool_groups: vec![
                String::from("filesystem.read"),
                String::from("filesystem.write"),
            ],
            harness: String::from("native"),
            harness_version: String::from("1.0.0"),
            harness_capability_set_hash: hash,
            harness_required_capabilities: Vec::new(),
            required_capabilities: Vec::new(),
            interceptor_order: Vec::new(),
            budgets: SessionStyleBudgets {
                max_iterations: 1,
                max_steps: 10,
                max_tokens: 1_000,
                max_cost_micros: 1_000,
                max_duration_ms: 1_000,
            },
            permission_defaults: SessionPermissionDefaults {
                default: String::from("ask"),
                groups: BTreeMap::new(),
            },
            child_agent_policy_json: String::from("{}"),
            retry_policy_json: String::from("{}"),
            termination_policy_json: String::from("{}"),
        }
    }

    fn inherited_mcp() -> SessionMcpBinding {
        SessionMcpBinding {
            schema_version: 1,
            declaration_hash: ContentHash::digest(b"mcp-declarations"),
            configuration_reference: Some(String::from("mcp-bootstrap.enc.json")),
            servers: vec![SessionMcpServerBinding {
                id: String::from("fixture"),
                display_name: String::from("Fixture"),
                transport: SessionMcpTransportBinding::StreamableHttp {
                    url: String::from("https://localhost.invalid/mcp"),
                    legacy_sse: false,
                    headers: Vec::new(),
                },
            }],
        }
    }

    #[test]
    fn child_binding_enforces_tools_context_budget_and_no_memory() {
        let command = command(ChildMemoryAccess::None);
        let mut binding = binding();

        restrict_child_binding(&mut binding, &command).expect("restriction");

        assert_eq!(binding.tool_groups, ["filesystem.read"]);
        assert_eq!(binding.budgets.max_tokens, 300);
        assert_eq!(binding.memory.provider, "none");
        assert!(binding.memory.scopes.is_empty());
        assert_eq!(binding.memory.retrieval_timing, "never");
        assert_eq!(binding.memory.write_policy, "never");
        assert_eq!(binding.memory.injection_location, "none");
    }

    #[test]
    fn child_binding_makes_read_only_memory_non_writable() {
        let command = command(ChildMemoryAccess::ReadOnly);
        let mut binding = binding();

        restrict_child_binding(&mut binding, &command).expect("restriction");

        assert_eq!(binding.memory.provider, "file");
        assert_eq!(binding.memory.write_policy, "never");
    }

    #[test]
    fn child_binding_inherits_exact_mcp_only_when_authorized() {
        let mut command = command(ChildMemoryAccess::None);
        command.tool_groups.push(String::from("mcp"));
        command.inherited_mcp = Some(inherited_mcp());
        let mut binding = binding();
        binding.tool_groups.push(String::from("mcp"));

        restrict_child_binding(&mut binding, &command).expect("restriction");

        assert_eq!(binding.mcp, inherited_mcp());
        assert!(validate_binding_selection(&binding, &command).is_ok());
    }

    #[test]
    fn child_binding_clears_style_mcp_when_inheritance_is_disabled() {
        let command = command(ChildMemoryAccess::None);
        let mut binding = binding();
        binding.mcp = inherited_mcp();

        restrict_child_binding(&mut binding, &command).expect("restriction");

        assert_eq!(binding.mcp, SessionMcpBinding::default());
    }

    #[test]
    fn recovered_binding_rejects_broader_capabilities() {
        let command = command(ChildMemoryAccess::None);
        let binding = binding();

        assert!(matches!(
            validate_binding_selection(&binding, &command),
            Err(ChildSessionLogicError::RecoveryIdentityMismatch)
        ));
    }

    #[test]
    fn sdk_child_memory_access_maps_explicitly_to_logic_contract() {
        assert_eq!(
            child_style_memory_access(ChildMemoryAccess::None),
            ChildStyleMemoryAccess::None
        );
        assert_eq!(
            child_style_memory_access(ChildMemoryAccess::ReadOnly),
            ChildStyleMemoryAccess::ReadOnly
        );
        assert_eq!(
            child_style_memory_access(ChildMemoryAccess::ReadWrite),
            ChildStyleMemoryAccess::ReadWrite
        );
    }

    #[test]
    fn shared_read_only_workspace_rejects_write_capable_tool_groups() {
        assert!(matches!(
            validate_workspace_tool_groups(
                &WorkspaceLeaseMode::SharedReadOnly,
                &[String::from("filesystem.write")]
            ),
            Err(ChildSessionLogicError::WorkspaceWriteCapability)
        ));
        assert!(matches!(
            validate_workspace_tool_groups(
                &WorkspaceLeaseMode::SharedReadOnly,
                &[String::from("process.shell")]
            ),
            Err(ChildSessionLogicError::WorkspaceWriteCapability)
        ));
    }

    #[test]
    fn isolated_workspace_allows_child_write_capability() {
        validate_workspace_tool_groups(
            &WorkspaceLeaseMode::IsolatedCopy,
            &[String::from("filesystem.write")],
        )
        .expect("isolated copy owns its writes");
    }
}
