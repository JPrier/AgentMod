//! Canonical runtime management of session-scoped plugin lifecycle state.

use std::{collections::BTreeSet, path::PathBuf};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{CausationId, ContentHash, Sequence, SessionId, Version};
use agentmod_runtime_data::{
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataError, EventIdentityDataPort},
    journal::JournalEventDataPort,
    plugin::{
        ActivatePluginsDataRequest, ChangePluginLifecycleDataRequest, PluginDataError,
        PluginDataPort, PluginLifecycleActionData,
    },
    registry::{ListSessionsDataRequest, SessionRegistryDataError, SessionRegistryDataPort},
};
use async_trait::async_trait;
use thiserror::Error;

use crate::{
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicError,
        SessionPersistenceLogicPort,
    },
    session::{
        PluginLifecycleChangeRequestedEvent, PluginLifecycleChangedEvent, PluginLifecycleRecord,
        RuntimeCommittedEvent, SessionState,
    },
};

const RUNTIME_PLUGIN_API_VERSION: &str = "1.0.0";

/// Runtime-owned plugin lifecycle transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginLifecycleAction {
    /// Stop future use while retaining plugin state.
    Disable,
    /// Restore a deliberately disabled plugin.
    Enable,
    /// Isolate a plugin after a policy, integrity, or crash finding.
    Quarantine,
    /// Release an explicitly quarantined plugin.
    Unquarantine,
}

impl PluginLifecycleAction {
    const fn action_name(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Enable => "enable",
            Self::Quarantine => "quarantine",
            Self::Unquarantine => "unquarantine",
        }
    }

    const fn terminal_state(self) -> &'static str {
        match self {
            Self::Disable => "disabled",
            Self::Enable | Self::Unquarantine => "active",
            Self::Quarantine => "quarantined",
        }
    }

    const fn to_data(self) -> PluginLifecycleActionData {
        match self {
            Self::Disable => PluginLifecycleActionData::Disable,
            Self::Enable => PluginLifecycleActionData::Enable,
            Self::Quarantine => PluginLifecycleActionData::Quarantine,
            Self::Unquarantine => PluginLifecycleActionData::Unquarantine,
        }
    }
}

/// Exact session-scoped lifecycle management command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangePluginLifecycleCommand {
    /// Canonical session storage root.
    pub sessions_root: PathBuf,
    /// Session whose immutable style selected the plugin.
    pub session_id: SessionId,
    /// Exact plugin identity.
    pub plugin_id: String,
    /// Requested transition.
    pub action: PluginLifecycleAction,
    /// Stable redacted reason required for quarantine.
    pub reason_code: Option<String>,
    /// Caller-selected cancellation lineage.
    pub cancellation_id: String,
}

/// Terminal canonical lifecycle result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangePluginLifecycleResult {
    /// Exact plugin identity.
    pub plugin_id: String,
    /// Exact selected plugin version.
    pub plugin_version: String,
    /// `disabled` or `quarantined`.
    pub state: String,
    /// Canonical terminal event sequence.
    pub committed_sequence: Sequence,
    /// Whether canonical replay already contained the exact terminal result.
    pub replayed: bool,
}

/// Startup reconciliation request for canonical pending lifecycle operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverPluginLifecyclesCommand {
    /// Canonical session storage root.
    pub sessions_root: PathBuf,
    /// Strict maximum number of sessions inspected during one startup.
    pub limit: usize,
}

/// Startup reconciliation summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverPluginLifecyclesResult {
    /// Sessions inspected.
    pub inspected_sessions: usize,
    /// Exact pending operations reconciled to terminal receipts.
    pub reconciled_operations: usize,
}

/// Runtime service-facing plugin lifecycle business port.
#[async_trait]
pub trait PluginLifecycleLogicPort: Send + Sync {
    /// Disables or quarantines one exact session plugin.
    ///
    /// The requested event commits before plugin-host I/O. A missing terminal
    /// event therefore blocks later plugin execution until the exact command is
    /// reconciled.
    ///
    /// # Errors
    ///
    /// Returns [`PluginLifecycleError`] for invalid identity, style mismatch,
    /// canonical conflicts, persistence failures, or host failures.
    async fn change_plugin_lifecycle(
        &self,
        command: ChangePluginLifecycleCommand,
    ) -> Result<ChangePluginLifecycleResult, PluginLifecycleError>;

    /// Reconciles already-requested lifecycle operations with their exact
    /// persisted cancellation identity.
    async fn recover_pending_plugin_lifecycles(
        &self,
        command: RecoverPluginLifecyclesCommand,
    ) -> Result<RecoverPluginLifecyclesResult, PluginLifecycleError> {
        let _ = command;
        Err(PluginLifecycleError::InvalidCommand)
    }
}

#[async_trait]
impl<D> PluginLifecycleLogicPort for crate::RuntimeLogic<D>
where
    D: Clone
        + EventIdentityDataPort
        + JournalEventDataPort
        + PluginDataPort
        + SessionRegistryDataPort
        + Send
        + Sync
        + 'static,
{
    #[allow(
        clippy::too_many_lines,
        reason = "the lifecycle use case keeps canonical intent, exact host dispatch, and terminal audit adjacent"
    )]
    async fn change_plugin_lifecycle(
        &self,
        command: ChangePluginLifecycleCommand,
    ) -> Result<ChangePluginLifecycleResult, PluginLifecycleError> {
        validate_command(&command)?;
        let session_directory = command.sessions_root.join(command.session_id.to_string());
        let persistence = SessionPersistenceLogic::new(self.data.clone());
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.clone(),
                expected_session_id: command.session_id,
            })
            .map_err(PluginLifecycleError::Persistence)?;
        let binding = loaded
            .state
            .style_binding
            .as_ref()
            .ok_or(PluginLifecycleError::StyleMigrationRequired)?;
        let compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&binding.compiled_style_json)
                .map_err(|_| PluginLifecycleError::StyleBindingInvalid)?;
        if !compiled.allowed_plugins.contains(&command.plugin_id) {
            return Err(PluginLifecycleError::PluginNotAllowed);
        }
        let plugin_version = self
            .data
            .plugin_version(&command.plugin_id)
            .map_err(PluginLifecycleError::Data)?;
        let configuration_reference = self
            .data
            .plugin_configuration_reference(&command.plugin_id)
            .map_err(PluginLifecycleError::Data)?;
        let request_digest =
            lifecycle_request_digest(&command, &plugin_version, configuration_reference)?;
        let should_append =
            if let Some(existing) = loaded.state.plugins.lifecycle.get(&command.plugin_id) {
                if exact_terminal(existing, &command, &plugin_version, request_digest) {
                    return terminal_result(&command.plugin_id, existing, true);
                }
                if exact_pending(existing, &command, &plugin_version, request_digest) {
                    false
                } else if valid_followup(existing, command.action) {
                    true
                } else {
                    return Err(PluginLifecycleError::LifecycleConflict);
                }
            } else {
                if matches!(
                    command.action,
                    PluginLifecycleAction::Enable | PluginLifecycleAction::Unquarantine
                ) {
                    return Err(PluginLifecycleError::LifecycleConflict);
                }
                true
            };
        if should_append {
            let requested = RuntimeCommittedEvent::PluginLifecycleChangeRequested(
                PluginLifecycleChangeRequestedEvent {
                    plugin_id: command.plugin_id.clone(),
                    plugin_version: plugin_version.clone(),
                    action: command.action.action_name().to_owned(),
                    reason_code: command.reason_code.clone(),
                    request_digest,
                    cancellation_id: command.cancellation_id.clone(),
                },
            );
            append_event(
                &self.data,
                &persistence,
                command.session_id,
                &session_directory,
                &loaded.state,
                loaded.last_event_id,
                requested,
            )?;
        }

        self.data
            .activate_plugins(ActivatePluginsDataRequest {
                session_id: command.session_id.to_string(),
                plugin_ids: compiled.allowed_plugins,
                runtime_api_version: String::from(RUNTIME_PLUGIN_API_VERSION),
                capabilities: compiled
                    .required_capabilities
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                cancellation_id: command.cancellation_id.clone(),
            })
            .await
            .map_err(PluginLifecycleError::Data)?;
        let changed = self
            .data
            .change_plugin_lifecycle(ChangePluginLifecycleDataRequest {
                session_id: command.session_id.to_string(),
                plugin_id: command.plugin_id.clone(),
                plugin_version: plugin_version.clone(),
                configuration_reference,
                action: command.action.to_data(),
                reason_code: command.reason_code.clone(),
                cancellation_id: command.cancellation_id.clone(),
            })
            .await
            .map_err(PluginLifecycleError::Data)?;
        if changed.plugin_id != command.plugin_id
            || changed.state != command.action.terminal_state()
            || changed.audit_operation != command.action.action_name()
            || changed.audit_outcome.trim().is_empty()
        {
            return Err(PluginLifecycleError::InvalidHostReceipt);
        }
        let reloaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.clone(),
                expected_session_id: command.session_id,
            })
            .map_err(PluginLifecycleError::Persistence)?;
        let pending = reloaded
            .state
            .plugins
            .lifecycle
            .get(&command.plugin_id)
            .ok_or(PluginLifecycleError::LifecycleConflict)?;
        if !exact_pending(pending, &command, &plugin_version, request_digest) {
            if exact_terminal(pending, &command, &plugin_version, request_digest) {
                return terminal_result(&command.plugin_id, pending, true);
            }
            return Err(PluginLifecycleError::LifecycleConflict);
        }
        let terminal = RuntimeCommittedEvent::PluginLifecycleChanged(PluginLifecycleChangedEvent {
            plugin_id: command.plugin_id.clone(),
            plugin_version: plugin_version.clone(),
            state: changed.state,
            reason_code: command.reason_code,
            request_digest,
            host_audit_operation: changed.audit_operation,
            host_audit_outcome: changed.audit_outcome,
        });
        let committed_sequence = append_event(
            &self.data,
            &persistence,
            command.session_id,
            &session_directory,
            &reloaded.state,
            reloaded.last_event_id,
            terminal,
        )?;
        Ok(ChangePluginLifecycleResult {
            plugin_id: command.plugin_id,
            plugin_version,
            state: command.action.terminal_state().to_owned(),
            committed_sequence,
            replayed: false,
        })
    }

    async fn recover_pending_plugin_lifecycles(
        &self,
        command: RecoverPluginLifecyclesCommand,
    ) -> Result<RecoverPluginLifecyclesResult, PluginLifecycleError> {
        if command.sessions_root.as_os_str().is_empty() || !(1..=10_000).contains(&command.limit) {
            return Err(PluginLifecycleError::InvalidCommand);
        }
        let sessions = self
            .data
            .list(ListSessionsDataRequest {
                sessions_root: command.sessions_root.clone(),
                limit: command.limit,
            })
            .map_err(PluginLifecycleError::Registry)?;
        let inspected_sessions = sessions.len();
        let mut reconciled_operations = 0;
        for session in sessions {
            let persistence = SessionPersistenceLogic::new(self.data.clone());
            let loaded = persistence
                .load_session(LoadSessionCommand {
                    session_directory: command.sessions_root.join(session.id.to_string()),
                    expected_session_id: session.id,
                })
                .map_err(PluginLifecycleError::Persistence)?;
            for (plugin_id, pending) in loaded
                .state
                .plugins
                .lifecycle
                .iter()
                .filter(|(_, lifecycle)| lifecycle.state == "pending")
            {
                if pending.cancellation_id.is_empty() {
                    return Err(PluginLifecycleError::LifecycleIdentityMigrationRequired {
                        session_id: session.id,
                        plugin_id: plugin_id.clone(),
                    });
                }
                let action = match pending.action.as_str() {
                    "disable" => PluginLifecycleAction::Disable,
                    "enable" => PluginLifecycleAction::Enable,
                    "quarantine" => PluginLifecycleAction::Quarantine,
                    "unquarantine" => PluginLifecycleAction::Unquarantine,
                    _ => return Err(PluginLifecycleError::LifecycleConflict),
                };
                self.change_plugin_lifecycle(ChangePluginLifecycleCommand {
                    sessions_root: command.sessions_root.clone(),
                    session_id: session.id,
                    plugin_id: plugin_id.clone(),
                    action,
                    reason_code: pending.reason_code.clone(),
                    cancellation_id: pending.cancellation_id.clone(),
                })
                .await?;
                reconciled_operations += 1;
            }
        }
        Ok(RecoverPluginLifecyclesResult {
            inspected_sessions,
            reconciled_operations,
        })
    }
}

fn validate_command(command: &ChangePluginLifecycleCommand) -> Result<(), PluginLifecycleError> {
    if command.sessions_root.as_os_str().is_empty()
        || !valid_identifier(&command.plugin_id)
        || !valid_identifier(&command.cancellation_id)
        || !matches!(command.action, PluginLifecycleAction::Quarantine)
            && command.reason_code.is_some()
        || matches!(command.action, PluginLifecycleAction::Quarantine)
            && command
                .reason_code
                .as_ref()
                .is_none_or(|reason| !valid_identifier(reason))
    {
        return Err(PluginLifecycleError::InvalidCommand);
    }
    Ok(())
}

fn valid_followup(existing: &PluginLifecycleRecord, action: PluginLifecycleAction) -> bool {
    existing.changed_at.is_some()
        && matches!(
            (existing.state.as_str(), action),
            (
                "active",
                PluginLifecycleAction::Disable | PluginLifecycleAction::Quarantine
            ) | ("disabled", PluginLifecycleAction::Enable)
                | ("quarantined", PluginLifecycleAction::Unquarantine)
        )
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn lifecycle_request_digest(
    command: &ChangePluginLifecycleCommand,
    plugin_version: &str,
    configuration_reference: ContentHash,
) -> Result<ContentHash, PluginLifecycleError> {
    serde_json::to_vec(&(
        "agentmod.plugin.lifecycle.request.v1",
        command.session_id,
        &command.plugin_id,
        plugin_version,
        configuration_reference,
        command.action.action_name(),
        &command.reason_code,
        &command.cancellation_id,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginLifecycleError::InvalidCommand)
}

fn exact_pending(
    existing: &PluginLifecycleRecord,
    command: &ChangePluginLifecycleCommand,
    plugin_version: &str,
    request_digest: ContentHash,
) -> bool {
    existing.plugin_version == plugin_version
        && existing.action == command.action.action_name()
        && existing.state == "pending"
        && existing.reason_code == command.reason_code
        && existing.request_digest == request_digest
        && existing.changed_at.is_none()
}

fn exact_terminal(
    existing: &PluginLifecycleRecord,
    command: &ChangePluginLifecycleCommand,
    plugin_version: &str,
    request_digest: ContentHash,
) -> bool {
    existing.plugin_version == plugin_version
        && existing.action == command.action.action_name()
        && existing.state == command.action.terminal_state()
        && existing.reason_code == command.reason_code
        && existing.request_digest == request_digest
        && existing.changed_at.is_some()
}

fn terminal_result(
    plugin_id: &str,
    existing: &PluginLifecycleRecord,
    replayed: bool,
) -> Result<ChangePluginLifecycleResult, PluginLifecycleError> {
    Ok(ChangePluginLifecycleResult {
        plugin_id: plugin_id.to_owned(),
        plugin_version: existing.plugin_version.clone(),
        state: existing.state.clone(),
        committed_sequence: existing
            .changed_at
            .ok_or(PluginLifecycleError::LifecycleConflict)?,
        replayed,
    })
}

fn append_event<D>(
    data: &D,
    persistence: &SessionPersistenceLogic<D>,
    session_id: SessionId,
    session_directory: &std::path::Path,
    state: &SessionState,
    expected_head_event_id: agentmod_primitives::EventId,
    payload: RuntimeCommittedEvent,
) -> Result<Sequence, PluginLifecycleError>
where
    D: EventIdentityDataPort + JournalEventDataPort,
{
    let sequence = state
        .last_sequence
        .checked_next()
        .map_err(|_| PluginLifecycleError::SequenceOverflow)?;
    let identity = data
        .allocate_event_identity(AllocateEventIdentityDataRequest)
        .map_err(PluginLifecycleError::Identity)?;
    let event = EventEnvelope::seal(
        EventMetadata {
            event_id: identity.event_id,
            scope: EventScope::Session(session_id),
            sequence,
            timestamp: identity.timestamp,
            event_type: payload.event_type().to_owned(),
            event_version: Version::new(1, 0),
            correlation_id: identity.correlation_id,
            causation_id: CausationId::from_uuid(expected_head_event_id.into_uuid()),
            parent_graph_node_id: None,
            origin: EventOrigin {
                subsystem: String::from("runtime"),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts: Vec::new(),
            classification: EventClassification::Committed,
        },
        payload,
    )
    .map_err(|_| PluginLifecycleError::Event)?;
    match persistence
        .compare_append_event(CompareAppendSessionEventCommand {
            session_directory: session_directory.to_owned(),
            expected_head_event_id,
            event,
            durability: CommitDurability::Data,
        })
        .map_err(PluginLifecycleError::Persistence)?
    {
        CompareAppendSessionEventResult::Appended(appended)
            if appended.event_id == identity.event_id && appended.sequence == sequence =>
        {
            Ok(sequence)
        }
        CompareAppendSessionEventResult::Appended(_) => Err(PluginLifecycleError::InvalidAppend),
        CompareAppendSessionEventResult::Conflict => Err(PluginLifecycleError::ConcurrentMutation),
    }
}

/// Plugin lifecycle business failure.
#[derive(Debug, Error)]
pub enum PluginLifecycleError {
    /// Request identity or action/reason shape is invalid.
    #[error("plugin lifecycle command is invalid")]
    InvalidCommand,
    /// Session binding predates immutable style selection.
    #[error("plugin lifecycle management requires an immutable style binding")]
    StyleMigrationRequired,
    /// Immutable compiled style cannot be decoded.
    #[error("plugin lifecycle management found an invalid style binding")]
    StyleBindingInvalid,
    /// Plugin was not explicitly allowed by the immutable style.
    #[error("plugin is not allowed by the immutable session style")]
    PluginNotAllowed,
    /// Another lifecycle command already owns the plugin.
    #[error("plugin lifecycle state conflicts with the requested transition")]
    LifecycleConflict,
    /// Runtime data operation failed.
    #[error("plugin lifecycle data operation failed: {0}")]
    Data(PluginDataError),
    /// Session catalog enumeration failed.
    #[error("plugin lifecycle session catalog is unavailable: {0}")]
    Registry(SessionRegistryDataError),
    /// A legacy pending event cannot be safely reconstructed.
    #[error(
        "PLUGLIFE001: session {session_id} plugin {plugin_id} has a legacy pending lifecycle request without a cancellation identity; explicitly migrate or replace the session"
    )]
    LifecycleIdentityMigrationRequired {
        /// Affected session.
        session_id: SessionId,
        /// Affected plugin.
        plugin_id: String,
    },
    /// Host result did not match the exact requested transition.
    #[error("plugin host returned an invalid lifecycle receipt")]
    InvalidHostReceipt,
    /// Canonical session persistence failed.
    #[error("plugin lifecycle persistence failed: {0}")]
    Persistence(SessionPersistenceLogicError),
    /// Event identity allocation failed.
    #[error("plugin lifecycle event identity is unavailable: {0}")]
    Identity(EventIdentityDataError),
    /// Event sequence cannot advance.
    #[error("plugin lifecycle event sequence overflow")]
    SequenceOverflow,
    /// Canonical event sealing failed.
    #[error("plugin lifecycle event could not be sealed")]
    Event,
    /// Journal advanced concurrently.
    #[error("plugin lifecycle journal advanced concurrently")]
    ConcurrentMutation,
    /// Persistence returned a mismatched append receipt.
    #[error("plugin lifecycle append receipt is invalid")]
    InvalidAppend,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(action: PluginLifecycleAction) -> ChangePluginLifecycleCommand {
        ChangePluginLifecycleCommand {
            sessions_root: PathBuf::from("sessions"),
            session_id: SessionId::from_uuid(uuid::Uuid::from_u128(1)),
            plugin_id: String::from("fixture.plugin"),
            action,
            reason_code: matches!(action, PluginLifecycleAction::Quarantine)
                .then(|| String::from("integrity_failure")),
            cancellation_id: String::from("lifecycle-cancellation-1"),
        }
    }

    #[test]
    fn lifecycle_request_digest_binds_action_reason_and_cancellation() {
        let disable = command(PluginLifecycleAction::Disable);
        let quarantine = command(PluginLifecycleAction::Quarantine);
        let configuration = ContentHash::digest(b"configuration");
        let disable_digest =
            lifecycle_request_digest(&disable, "1.0.0", configuration).expect("disable digest");
        assert_ne!(
            disable_digest,
            lifecycle_request_digest(&quarantine, "1.0.0", configuration)
                .expect("quarantine digest")
        );
        let mut changed_cancellation = disable.clone();
        changed_cancellation.cancellation_id = String::from("lifecycle-cancellation-2");
        assert_ne!(
            disable_digest,
            lifecycle_request_digest(&changed_cancellation, "1.0.0", configuration)
                .expect("changed cancellation digest")
        );
        assert_ne!(
            disable_digest,
            lifecycle_request_digest(&disable, "2.0.0", configuration)
                .expect("changed version digest")
        );
        assert_ne!(
            disable_digest,
            lifecycle_request_digest(
                &disable,
                "1.0.0",
                ContentHash::digest(b"changed configuration")
            )
            .expect("changed configuration digest")
        );
    }

    #[test]
    fn pending_and_terminal_reconciliation_require_exact_request() {
        let command = command(PluginLifecycleAction::Quarantine);
        let request_digest =
            lifecycle_request_digest(&command, "1.0.0", ContentHash::digest(b"configuration"))
                .expect("request digest");
        let mut record = PluginLifecycleRecord {
            plugin_version: String::from("1.0.0"),
            action: String::from("quarantine"),
            state: String::from("pending"),
            reason_code: Some(String::from("integrity_failure")),
            request_digest,
            cancellation_id: String::from("lifecycle-cancellation-1"),
            requested_at: Sequence::new(2).expect("sequence"),
            changed_at: None,
        };
        assert!(exact_pending(&record, &command, "1.0.0", request_digest));
        record.state = String::from("quarantined");
        record.changed_at = Some(Sequence::new(3).expect("sequence"));
        assert!(exact_terminal(&record, &command, "1.0.0", request_digest));
        let mut substituted = command;
        substituted.reason_code = Some(String::from("policy_failure"));
        assert!(!exact_terminal(
            &record,
            &substituted,
            "1.0.0",
            request_digest
        ));
    }

    #[test]
    fn disable_rejects_reason_and_quarantine_requires_one() {
        let mut disable = command(PluginLifecycleAction::Disable);
        disable.reason_code = Some(String::from("unexpected"));
        assert!(matches!(
            validate_command(&disable),
            Err(PluginLifecycleError::InvalidCommand)
        ));
        let mut quarantine = command(PluginLifecycleAction::Quarantine);
        quarantine.reason_code = None;
        assert!(matches!(
            validate_command(&quarantine),
            Err(PluginLifecycleError::InvalidCommand)
        ));
    }

    #[test]
    fn reactivation_requires_the_exact_terminal_source_state() {
        let digest = ContentHash::digest(b"request");
        let terminal = |state: &str| PluginLifecycleRecord {
            plugin_version: String::from("1.0.0"),
            action: String::from("prior"),
            state: state.to_owned(),
            reason_code: None,
            request_digest: digest,
            cancellation_id: String::from("lifecycle-cancellation-1"),
            requested_at: Sequence::new(1).expect("sequence"),
            changed_at: Some(Sequence::new(2).expect("sequence")),
        };
        assert!(valid_followup(
            &terminal("disabled"),
            PluginLifecycleAction::Enable
        ));
        assert!(valid_followup(
            &terminal("quarantined"),
            PluginLifecycleAction::Unquarantine
        ));
        assert!(!valid_followup(
            &terminal("quarantined"),
            PluginLifecycleAction::Enable
        ));
        assert!(!valid_followup(
            &terminal("disabled"),
            PluginLifecycleAction::Unquarantine
        ));
        assert!(valid_followup(
            &terminal("active"),
            PluginLifecycleAction::Disable
        ));
        assert!(valid_followup(
            &terminal("active"),
            PluginLifecycleAction::Quarantine
        ));
    }

    #[test]
    fn enable_and_unquarantine_reject_reason_codes() {
        for action in [
            PluginLifecycleAction::Enable,
            PluginLifecycleAction::Unquarantine,
        ] {
            let mut value = command(action);
            value.reason_code = Some(String::from("not_allowed"));
            assert!(matches!(
                validate_command(&value),
                Err(PluginLifecycleError::InvalidCommand)
            ));
        }
    }
}
