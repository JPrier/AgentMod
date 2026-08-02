//! Durable coordination for plugin-node preserved-state writes.
//!
//! Raw plugin state crosses the plugin-host persistence boundary, but never
//! enters the canonical session journal. The journal retains only the exact
//! terminal CAS receipt and its hashes. A journal conflict therefore reloads
//! and reclassifies the same receipt without repeating the external write.

use std::{fmt, path::PathBuf};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{
    CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId, TimestampMillis, Version,
};
use agentmod_runtime_data::{
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
    plugin::PluginDataError,
};
use thiserror::Error;

use crate::{
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort,
    },
    plugin::{
        LoadPluginNodeStateCommand, LoadedPluginNodeState, PersistPluginNodeStateCommand,
        PluginNodeStatePersistenceError, PluginNodeStatePersistenceLogicPort,
        PluginNodeStateReadError, PluginNodeStateReadLogicPort,
        PluginNodeStateScope as PersistenceStateScope, plugin_invocation_cancellation_target,
        plugin_node_state_persistence_digests, plugin_node_state_persistence_request_hash,
        plugin_node_state_read_digests, plugin_node_state_value_hash,
    },
    session::{
        PluginNodeActionTerminalRecord, PluginNodeInvocationIdentity, PluginNodeInvocationState,
        PluginNodeStateFailureDisposition, PluginNodeStatePersistenceFailedEvent,
        PluginNodeStatePreservationDisposition, PreparedPluginNodeStatePreservation,
        RuntimeCommittedEvent, SessionReducerError, SessionState,
        classify_plugin_node_state_failure, classify_plugin_node_state_preservation,
        derive_plugin_node_state_prior, plugin_node_state_failure_diagnostic_hash,
        plugin_node_state_idempotency_key, prepare_plugin_node_state_preservation, reduce,
    },
};

const MAX_APPEND_RETRIES: usize = 32;

/// Exact immutable input for loading a prior plugin-owned state value.
#[derive(Clone, Debug)]
pub struct LoadPriorPluginNodeStateCommand {
    /// Canonical owning session.
    pub session_id: SessionId,
    /// Immutable node invocation identity material.
    pub identity: PluginNodeInvocationIdentity,
    /// Exact selected plugin version obtained from declaration lookup.
    pub plugin_version: String,
    /// Immutable plugin activation configuration reference.
    pub plugin_configuration_reference: ContentHash,
    /// Declaration scope selected by the exact executor.
    pub state_scope: PersistenceStateScope,
    /// Runtime-owned cancellation identity propagated to plugin-host.
    pub cancellation_id: String,
}

/// Prior state made available to one isolated invocation.
#[derive(Clone, PartialEq)]
pub enum PriorPluginNodeState {
    /// Invocation scope, or a Session scope before its first canonical write.
    None,
    /// Exact raw state corresponding to the canonical generation and hash.
    Loaded {
        /// Canonical generation read from plugin-host.
        generation: u64,
        /// Canonical hash used to validate the bounded raw state.
        state_hash: ContentHash,
        /// Raw bounded plugin-owned state. This value is never journaled.
        state: serde_json::Value,
    },
}

impl fmt::Debug for PriorPluginNodeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::Loaded {
                generation,
                state_hash,
                ..
            } => formatter
                .debug_struct("Loaded")
                .field("generation", generation)
                .field("state_hash", state_hash)
                .field("state", &"[redacted]")
                .finish(),
        }
    }
}

/// Pure-replay plus authenticated-read coordinator for prior plugin state.
pub struct PluginStateReadTurnCoordinator<J, R> {
    journal: J,
    reader: R,
}

impl<J, R> PluginStateReadTurnCoordinator<J, R> {
    /// Composes canonical replay with the immediately-lower state-read port.
    #[must_use]
    pub const fn new(journal: J, reader: R) -> Self {
        Self { journal, reader }
    }
}

impl<J, R> PluginStateReadTurnCoordinator<J, R>
where
    J: PluginStateTurnJournal,
    R: PluginNodeStateReadLogicPort,
{
    /// Loads the exact raw Session state matching canonical predecessor hashes.
    ///
    /// Invocation scope never performs a read. Unsupported scopes fail before
    /// crossing the plugin-host boundary. The read port validates the complete
    /// terminal receipt and this coordinator performs no retry or journal write.
    ///
    /// # Errors
    ///
    /// Fails closed when replay identity, scope, cancellation, predecessor, or
    /// the authenticated state-read boundary is unavailable or ambiguous.
    pub async fn load(
        &self,
        command: LoadPriorPluginNodeStateCommand,
    ) -> Result<PriorPluginNodeState, PluginStateReadTurnError> {
        if !valid_identifier(&command.cancellation_id) {
            return Err(PluginStateReadTurnError::InvalidCancellation);
        }
        match command.state_scope {
            PersistenceStateScope::Invocation => return Ok(PriorPluginNodeState::None),
            PersistenceStateScope::Session => {}
            PersistenceStateScope::ModelCall
            | PersistenceStateScope::Turn
            | PersistenceStateScope::Project
            | PersistenceStateScope::User
            | PersistenceStateScope::Runtime => {
                return Err(PluginStateReadTurnError::UnsupportedScope);
            }
        }
        let head = self.journal.load()?;
        if head.state.id != command.session_id {
            return Err(PluginStateReadTurnError::SessionMismatch);
        }
        let prior =
            derive_plugin_node_state_prior(&head.state, &command.identity, command.state_scope)?;
        let Some(state_hash) = prior.state_hash else {
            if prior.generation == 0 {
                return Ok(PriorPluginNodeState::None);
            }
            return Err(PluginStateReadTurnError::InvalidPrior);
        };
        if prior.generation == 0 {
            return Err(PluginStateReadTurnError::InvalidPrior);
        }
        let mut read = build_state_read_command(&command, prior.generation, state_hash)?;
        let (action_digest, authorization_digest) =
            plugin_node_state_read_digests(&read).map_err(PluginStateReadTurnError::Read)?;
        read.action_digest = action_digest;
        read.authorization_digest = authorization_digest;
        let LoadedPluginNodeState { state, receipt } = self
            .reader
            .load_plugin_node_state(read)
            .await
            .map_err(PluginStateReadTurnError::Read)?;
        if receipt.generation != prior.generation
            || receipt.state_hash != state_hash
            || plugin_node_state_value_hash(&state)
                .map_err(|_| PluginStateReadTurnError::InvalidPrior)?
                != state_hash
        {
            return Err(PluginStateReadTurnError::InvalidPrior);
        }
        Ok(PriorPluginNodeState::Loaded {
            generation: prior.generation,
            state_hash,
            state,
        })
    }
}

fn build_state_read_command(
    command: &LoadPriorPluginNodeStateCommand,
    generation: u64,
    state_hash: ContentHash,
) -> Result<LoadPluginNodeStateCommand, PluginStateReadTurnError> {
    let operation_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            command.session_id,
            &command.identity.work,
            &command.identity.executor,
            &command.identity.plugin_id,
            command.state_scope,
            generation,
            state_hash,
        ))
        .map_err(|_| PluginStateReadTurnError::Identity)?,
    );
    let suffix = operation_digest.to_hex();
    let invocation_id = format!("plugin-state-read:{suffix}");
    let operation_id = format!("{}:state-read", command.identity.executor.executor_id);
    let idempotency_key = format!("plugin-state-read-{suffix}");
    let request_hash = serde_json::to_vec(&(
        "agentmod.plugin.node-state.load.request.v1",
        &command.identity.plugin_id,
        &invocation_id,
        operation_digest,
        &command.identity.executor.executor_id,
        &command.identity.executor.executor_version,
        command.identity.executor.executor_declaration_hash,
        command.plugin_configuration_reference,
        state_scope_name(command.state_scope),
        generation,
        state_hash,
        &idempotency_key,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginStateReadTurnError::Identity)?;
    let cancellation_target = plugin_invocation_cancellation_target(
        &command.session_id.to_string(),
        &command.identity.work.run_id,
        &command.identity.plugin_id,
        &command.plugin_version,
        &invocation_id,
        &operation_id,
        command.identity.executor.executor_declaration_hash,
        request_hash,
    )
    .map_err(|_| PluginStateReadTurnError::Identity)?;
    Ok(LoadPluginNodeStateCommand {
        cancellation_target,
        session_id: command.session_id.to_string(),
        plugin_id: command.identity.plugin_id.clone(),
        invocation_id,
        invocation_digest: operation_digest,
        executor_id: command.identity.executor.executor_id.clone(),
        executor_version: command.identity.executor.executor_version.clone(),
        executor_declaration_hash: command.identity.executor.executor_declaration_hash,
        configuration_reference: command.plugin_configuration_reference,
        state_scope: command.state_scope,
        expected_generation: generation,
        expected_state_hash: state_hash,
        action_digest: ContentHash::from_bytes([0; 32]),
        authorization_digest: ContentHash::from_bytes([0; 32]),
        nonce: format!("plugin-state-read-nonce-{suffix}"),
        cancellation_id: command.cancellation_id.clone(),
        idempotency_key,
    })
}

/// Stable fail-closed prior-state read error.
#[derive(Debug, Error)]
pub enum PluginStateReadTurnError {
    /// Declaration scope lacks a canonical persistence identity.
    #[error("plugin state read scope is unsupported")]
    UnsupportedScope,
    /// Runtime cancellation identity is invalid.
    #[error("plugin state read cancellation identity is invalid")]
    InvalidCancellation,
    /// Canonical session differs from the requested session.
    #[error("plugin state read session identity is invalid")]
    SessionMismatch,
    /// Canonical predecessor generation/hash is inconsistent.
    #[error("plugin state read canonical predecessor is invalid")]
    InvalidPrior,
    /// Stable operation identity could not be encoded.
    #[error("plugin state read identity could not be derived")]
    Identity,
    /// Canonical session load or replay failed.
    #[error("plugin state read journal failed: {0}")]
    Journal(#[from] PluginStateTurnJournalError),
    /// Canonical replay rejected the state chain.
    #[error("plugin state read replay failed: {0}")]
    Replay(#[from] SessionReducerError),
    /// Authenticated plugin-host read failed closed.
    #[error("plugin state read failed: {0}")]
    Read(PluginNodeStateReadError),
}

/// Exact canonical head used to prepare one hash-only state event.
#[derive(Clone, Debug)]
pub struct PluginStateTurnHead {
    /// Pure replay projection.
    pub state: SessionState,
    /// Exact canonical journal head.
    pub last_event_id: EventId,
}

/// Runtime-owned event identity and recorded time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginStateTurnEventIdentity {
    /// Fresh canonical event identity.
    pub event_id: EventId,
    /// Runtime-recorded event time.
    pub timestamp: TimestampMillis,
    /// Session/run correlation identity.
    pub correlation_id: CorrelationId,
}

/// Exact expected journal position for compare-and-append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginStateTurnAppendPosition {
    /// Current canonical sequence.
    pub sequence: Sequence,
    /// Current canonical event identity.
    pub event_id: EventId,
}

/// Outcome of one canonical compare-and-append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginStateTurnAppendOutcome {
    /// The exact event was appended.
    Appended,
    /// Another writer advanced the journal.
    Conflict,
}

/// Stable journal boundary failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("plugin state turn journal failed: {code}")]
pub struct PluginStateTurnJournalError {
    /// Bounded diagnostic code.
    pub code: String,
}

/// Narrow canonical journal boundary for plugin-state coordination.
pub trait PluginStateTurnJournal: Send + Sync + 'static {
    /// Loads and purely replays the current session.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when load or replay fails.
    fn load(&self) -> Result<PluginStateTurnHead, PluginStateTurnJournalError>;

    /// Allocates runtime-owned canonical event identity.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when identity allocation fails.
    fn allocate_identity(
        &self,
    ) -> Result<PluginStateTurnEventIdentity, PluginStateTurnJournalError>;

    /// Atomically appends at the exact expected head.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error for persistence or receipt mismatch.
    fn append(
        &self,
        expected: PluginStateTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<PluginStateTurnAppendOutcome, PluginStateTurnJournalError>;
}

/// Production session-journal adapter.
#[derive(Clone, Debug)]
pub struct SessionPluginStateTurnJournal<D> {
    data: D,
    persistence: SessionPersistenceLogic<D>,
    session_id: SessionId,
    session_directory: PathBuf,
}

impl<D> SessionPluginStateTurnJournal<D>
where
    D: Clone,
{
    /// Binds one immutable session journal.
    #[must_use]
    pub fn new(data: D, session_id: SessionId, session_directory: PathBuf) -> Self {
        Self {
            persistence: SessionPersistenceLogic::new(data.clone()),
            data,
            session_id,
            session_directory,
        }
    }
}

impl<D> PluginStateTurnJournal for SessionPluginStateTurnJournal<D>
where
    D: Clone + EventIdentityDataPort + JournalEventDataPort + Send + Sync + 'static,
{
    fn load(&self) -> Result<PluginStateTurnHead, PluginStateTurnJournalError> {
        self.persistence
            .load_session(LoadSessionCommand {
                session_directory: self.session_directory.clone(),
                expected_session_id: self.session_id,
            })
            .map(|loaded| PluginStateTurnHead {
                state: loaded.state,
                last_event_id: loaded.last_event_id,
            })
            .map_err(|_| journal_error("load_failed"))
    }

    fn allocate_identity(
        &self,
    ) -> Result<PluginStateTurnEventIdentity, PluginStateTurnJournalError> {
        self.data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map(|identity| PluginStateTurnEventIdentity {
                event_id: identity.event_id,
                timestamp: identity.timestamp,
                correlation_id: identity.correlation_id,
            })
            .map_err(|_| journal_error("identity_unavailable"))
    }

    fn append(
        &self,
        expected: PluginStateTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<PluginStateTurnAppendOutcome, PluginStateTurnJournalError> {
        let event_id = event.metadata.event_id;
        let sequence = event.metadata.sequence;
        if expected.sequence.checked_next().ok() != Some(sequence) {
            return Err(journal_error("append_sequence_invalid"));
        }
        match self
            .persistence
            .compare_append_event(CompareAppendSessionEventCommand {
                session_directory: self.session_directory.clone(),
                expected_head_event_id: expected.event_id,
                event,
                durability: CommitDurability::Data,
            })
            .map_err(|_| journal_error("append_failed"))?
        {
            CompareAppendSessionEventResult::Appended(appended)
                if appended.event_id == event_id && appended.sequence == sequence =>
            {
                Ok(PluginStateTurnAppendOutcome::Appended)
            }
            CompareAppendSessionEventResult::Appended(_) => {
                Err(journal_error("append_receipt_mismatch"))
            }
            CompareAppendSessionEventResult::Conflict => Ok(PluginStateTurnAppendOutcome::Conflict),
        }
    }
}

fn journal_error(code: &'static str) -> PluginStateTurnJournalError {
    PluginStateTurnJournalError {
        code: code.to_owned(),
    }
}

/// Exact validated plugin application whose bounded state should be retained.
#[derive(Clone, Debug)]
pub struct PreservePluginNodeStateCommand {
    /// Canonical session.
    pub session_id: SessionId,
    /// Immutable invocation and resolved-executor identity.
    pub identity: PluginNodeInvocationIdentity,
    /// Exact selected plugin version obtained from declaration lookup.
    pub plugin_version: String,
    /// Immutable plugin activation configuration reference.
    pub plugin_configuration_reference: ContentHash,
    /// Exact outcome-validation hash authorizing application.
    pub validation_hash: ContentHash,
    /// Raw bounded plugin-owned state. This never enters the journal.
    pub state: serde_json::Value,
    /// Declared state scope. Only invocation and session are supported.
    pub state_scope: PersistenceStateScope,
    /// Actual bounded Turn cancellation identity propagated to plugin-host.
    pub cancellation_id: String,
}

/// Whether a call appended or recovered the exact canonical receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginStateTurnStatus {
    /// This call appended the hash-only canonical event.
    Committed,
    /// Replay already contained the exact hash-only state result.
    AlreadyCommitted,
}

/// Terminal result of plugin-state coordination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreservePluginNodeStateResult {
    /// Canonical state generation.
    pub generation: u64,
    /// Exact state hash.
    pub state_hash: ContentHash,
    /// Stable terminal plugin-host receipt identity.
    pub terminal_receipt_id: String,
    /// Commit/recovery classification.
    pub status: PluginStateTurnStatus,
}

/// Coordinates one external state CAS and its hash-only canonical event.
pub struct PluginStateTurnCoordinator<J, P> {
    journal: J,
    persistence: P,
}

impl<J, P> PluginStateTurnCoordinator<J, P> {
    /// Composes the journal and immediately-lower plugin persistence use case.
    #[must_use]
    pub const fn new(journal: J, persistence: P) -> Self {
        Self {
            journal,
            persistence,
        }
    }
}

impl<J, P> PluginStateTurnCoordinator<J, P>
where
    J: PluginStateTurnJournal,
    P: PluginNodeStatePersistenceLogicPort,
{
    /// Persists bounded plugin state once, then commits its exact receipt.
    ///
    /// Journal CAS conflicts only reload and reclassify the retained receipt.
    /// An ambiguous external persistence result is terminal for this call and
    /// is never retried automatically.
    ///
    /// # Errors
    ///
    /// Fails closed for stale/substituted replay, unsupported scope, invalid
    /// state, ambiguous persistence, conflicting receipts, reducer rejection,
    /// journal failure, or exhausted journal conflicts.
    pub async fn preserve(
        &self,
        command: PreservePluginNodeStateCommand,
    ) -> Result<PreservePluginNodeStateResult, PluginStateTurnError> {
        if !matches!(
            command.state_scope,
            PersistenceStateScope::Invocation | PersistenceStateScope::Session
        ) {
            return Err(PluginStateTurnError::UnsupportedScope);
        }
        if !valid_identifier(&command.cancellation_id) {
            return Err(PluginStateTurnError::InvalidCancellation);
        }
        let state_hash = plugin_node_state_value_hash(&command.state)
            .map_err(PluginStateTurnError::Persistence)?;
        let head = self.journal.load()?;
        match preflight(&head.state, &command, state_hash)? {
            Preflight::AlreadyCommitted(result) => return Ok(result),
            Preflight::TerminalFailure(failed) => {
                return Err(terminal_failure_error(&failed));
            }
            Preflight::Ready => {}
        }
        let prior =
            derive_plugin_node_state_prior(&head.state, &command.identity, command.state_scope)?;
        let persistence_command =
            build_persistence_command(&command, state_hash, prior.generation, prior.state_hash)?;

        // This is deliberately the only external call in this coordinator
        // invocation. Everything below operates on the retained receipt.
        let receipt = match self
            .persistence
            .persist_plugin_node_state(persistence_command.clone())
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                let (code, ambiguous) =
                    classify_state_persistence_error(&persistence_command, &error)?;
                let failed = prepare_failure_event(
                    &head,
                    &command,
                    &persistence_command,
                    prior.generation,
                    prior.state_hash,
                    state_hash,
                    code,
                    ambiguous,
                )?;
                self.commit_failure(head, &failed)?;
                return Err(terminal_failure_error(&failed));
            }
        };

        let Ok(prepared) =
            prepare_plugin_node_state_preservation(&head.state, &persistence_command, &receipt)
        else {
            let failed = prepare_failure_event(
                &head,
                &command,
                &persistence_command,
                prior.generation,
                prior.state_hash,
                state_hash,
                "state_receipt_invalid",
                true,
            )?;
            self.commit_failure(head, &failed)?;
            return Err(terminal_failure_error(&failed));
        };
        let event = match prepared {
            PreparedPluginNodeStatePreservation::Append(event) => event,
            PreparedPluginNodeStatePreservation::AlreadyCommitted(event) => {
                return Ok(result_from_event(
                    &event,
                    PluginStateTurnStatus::AlreadyCommitted,
                ));
            }
            PreparedPluginNodeStatePreservation::Conflict => {
                return Err(PluginStateTurnError::ConflictingReceipt);
            }
            PreparedPluginNodeStatePreservation::InvalidOrder => {
                return Err(PluginStateTurnError::InvalidApplication);
            }
        };
        self.commit_receipt(head, &event)
            .map(|status| PreservePluginNodeStateResult {
                generation: receipt.generation,
                state_hash: receipt.state_hash,
                terminal_receipt_id: receipt.receipt_id,
                status,
            })
    }

    fn commit_receipt(
        &self,
        mut head: PluginStateTurnHead,
        event: &crate::session::PluginNodeStatePreservedEvent,
    ) -> Result<PluginStateTurnStatus, PluginStateTurnError> {
        for _ in 0..MAX_APPEND_RETRIES {
            match classify_plugin_node_state_preservation(&head.state, event) {
                PluginNodeStatePreservationDisposition::AlreadyCommitted => {
                    return Ok(PluginStateTurnStatus::AlreadyCommitted);
                }
                PluginNodeStatePreservationDisposition::Conflict => {
                    return Err(PluginStateTurnError::ConflictingReceipt);
                }
                PluginNodeStatePreservationDisposition::InvalidOrder => {
                    return Err(PluginStateTurnError::InvalidApplication);
                }
                PluginNodeStatePreservationDisposition::Append => {}
            }
            let identity = self.journal.allocate_identity()?;
            let sequence = head
                .state
                .last_sequence
                .checked_next()
                .map_err(|_| PluginStateTurnError::Sequence)?;
            let envelope = seal_event(&head, sequence, identity, event.clone())?;
            reduce(Some(head.state.clone()), &envelope)?;
            match self.journal.append(
                PluginStateTurnAppendPosition {
                    sequence: head.state.last_sequence,
                    event_id: head.last_event_id,
                },
                envelope,
            )? {
                PluginStateTurnAppendOutcome::Appended => {
                    return Ok(PluginStateTurnStatus::Committed);
                }
                PluginStateTurnAppendOutcome::Conflict => {
                    head = self.journal.load()?;
                }
            }
        }
        Err(PluginStateTurnError::AppendConflictLimit)
    }

    fn commit_failure(
        &self,
        mut head: PluginStateTurnHead,
        failed: &PluginNodeStatePersistenceFailedEvent,
    ) -> Result<PluginStateTurnStatus, PluginStateTurnError> {
        for _ in 0..MAX_APPEND_RETRIES {
            match classify_plugin_node_state_failure(&head.state, failed) {
                PluginNodeStateFailureDisposition::AlreadyCommitted => {
                    return Ok(PluginStateTurnStatus::AlreadyCommitted);
                }
                PluginNodeStateFailureDisposition::Conflict => {
                    return Err(PluginStateTurnError::ConflictingReceipt);
                }
                PluginNodeStateFailureDisposition::InvalidOrder => {
                    return Err(PluginStateTurnError::InvalidApplication);
                }
                PluginNodeStateFailureDisposition::Append => {}
            }
            let identity = self.journal.allocate_identity()?;
            let sequence = head
                .state
                .last_sequence
                .checked_next()
                .map_err(|_| PluginStateTurnError::Sequence)?;
            let envelope = seal_failure_event(&head, sequence, identity, failed.clone())?;
            reduce(Some(head.state.clone()), &envelope)?;
            match self.journal.append(
                PluginStateTurnAppendPosition {
                    sequence: head.state.last_sequence,
                    event_id: head.last_event_id,
                },
                envelope,
            )? {
                PluginStateTurnAppendOutcome::Appended => {
                    return Ok(PluginStateTurnStatus::Committed);
                }
                PluginStateTurnAppendOutcome::Conflict => {
                    head = self.journal.load()?;
                }
            }
        }
        Err(PluginStateTurnError::AppendConflictLimit)
    }
}

enum Preflight {
    Ready,
    AlreadyCommitted(PreservePluginNodeStateResult),
    TerminalFailure(Box<PluginNodeStatePersistenceFailedEvent>),
}

#[allow(
    clippy::too_many_arguments,
    reason = "the terminal marker explicitly binds every canonical predecessor and state-operation identity"
)]
fn prepare_failure_event(
    head: &PluginStateTurnHead,
    command: &PreservePluginNodeStateCommand,
    persistence_command: &PersistPluginNodeStateCommand,
    prior_generation: u64,
    prior_state_hash: Option<ContentHash>,
    state_hash: ContentHash,
    code: &str,
    ambiguous: bool,
) -> Result<PluginNodeStatePersistenceFailedEvent, PluginStateTurnError> {
    let application = head
        .state
        .style_execution
        .as_ref()
        .and_then(|execution| {
            execution
                .plugin_node_invocations
                .get(&command.identity.invocation_id)
        })
        .and_then(|record| record.outcome_application.as_deref())
        .ok_or(PluginStateTurnError::InvalidApplication)?;
    Ok(PluginNodeStatePersistenceFailedEvent {
        version: 1,
        identity: command.identity.clone(),
        validation_hash: command.validation_hash,
        prior_event_id: application.latest_event_id,
        state_hash,
        state_scope: map_scope(command.state_scope),
        executor_id: command.identity.executor.executor_id.clone(),
        executor_version: command.identity.executor.executor_version.clone(),
        executor_declaration_hash: command.identity.executor.executor_declaration_hash,
        prior_generation,
        prior_state_hash,
        idempotency_key: persistence_command.idempotency_key.clone(),
        code: code.to_owned(),
        ambiguous,
        diagnostic_hash: plugin_node_state_failure_diagnostic_hash(code, ambiguous)?,
    })
}

fn preflight(
    state: &SessionState,
    command: &PreservePluginNodeStateCommand,
    state_hash: ContentHash,
) -> Result<Preflight, PluginStateTurnError> {
    if state.id != command.session_id {
        return Err(PluginStateTurnError::SessionMismatch);
    }
    let record = state
        .style_execution
        .as_ref()
        .and_then(|execution| {
            execution
                .plugin_node_invocations
                .get(&command.identity.invocation_id)
        })
        .ok_or(PluginStateTurnError::InvalidApplication)?;
    if record.identity != command.identity {
        return Err(PluginStateTurnError::InvocationSubstitution);
    }
    let application = record
        .outcome_application
        .as_deref()
        .ok_or(PluginStateTurnError::InvalidApplication)?;
    if record.state != PluginNodeInvocationState::Completed
        || application.validated.validation_hash != command.validation_hash
        || application.validated.preserved_state_hash != state_hash
        || application.budget_charge.is_none()
        || application.actions.len() != application.validated.action_hashes.len()
        || !application.actions.iter().all(|action| {
            matches!(
                action.terminal,
                Some(PluginNodeActionTerminalRecord::Applied(_))
            )
        })
    {
        return Err(PluginStateTurnError::InvalidApplication);
    }
    if let Some(failed) = application.state_failure.as_deref() {
        if failed.identity != command.identity
            || failed.validation_hash != command.validation_hash
            || failed.state_hash != state_hash
            || failed.state_scope != map_scope(command.state_scope)
            || classify_plugin_node_state_failure(state, failed)
                != PluginNodeStateFailureDisposition::AlreadyCommitted
        {
            return Err(PluginStateTurnError::ConflictingReceipt);
        }
        return Ok(Preflight::TerminalFailure(Box::new(failed.clone())));
    }
    let Some(existing) = application.preserved_state.as_ref() else {
        return Ok(Preflight::Ready);
    };
    if existing.identity != command.identity
        || existing.validation_hash != command.validation_hash
        || existing.state_hash != state_hash
        || existing.state_scope != map_scope(command.state_scope)
        || classify_plugin_node_state_preservation(state, existing)
            != PluginNodeStatePreservationDisposition::AlreadyCommitted
    {
        return Err(PluginStateTurnError::ConflictingReceipt);
    }
    Ok(Preflight::AlreadyCommitted(result_from_event(
        existing,
        PluginStateTurnStatus::AlreadyCommitted,
    )))
}

fn build_persistence_command(
    command: &PreservePluginNodeStateCommand,
    state_hash: ContentHash,
    prior_generation: u64,
    prior_state_hash: Option<ContentHash>,
) -> Result<PersistPluginNodeStateCommand, PluginStateTurnError> {
    let idempotency_key = plugin_node_state_idempotency_key(
        command.session_id,
        &command.identity,
        command.validation_hash,
        command.state_scope,
        prior_generation,
        prior_state_hash,
        state_hash,
    )?;
    let suffix = idempotency_key
        .strip_prefix("plugin-state-write-")
        .ok_or(PluginStateTurnError::Identity)?;
    let operation_id = format!("{}:state-write", command.identity.executor.executor_id);
    let cancellation_target = plugin_invocation_cancellation_target(
        &command.session_id.to_string(),
        &command.identity.work.run_id,
        &command.identity.plugin_id,
        &command.plugin_version,
        &command.identity.invocation_id,
        &operation_id,
        command.identity.executor.executor_declaration_hash,
        ContentHash::from_bytes([0; 32]),
    )
    .map_err(|_| PluginStateTurnError::Identity)?;
    let mut persistence = PersistPluginNodeStateCommand {
        cancellation_target,
        session_id: command.session_id.to_string(),
        plugin_id: command.identity.plugin_id.clone(),
        invocation_id: command.identity.invocation_id.clone(),
        invocation_digest: command.identity.invocation_digest,
        executor_id: command.identity.executor.executor_id.clone(),
        executor_version: command.identity.executor.executor_version.clone(),
        executor_declaration_hash: command.identity.executor.executor_declaration_hash,
        configuration_reference: command.plugin_configuration_reference,
        state_scope: command.state_scope,
        prior_generation,
        prior_state_hash,
        state: command.state.clone(),
        state_hash,
        action_digest: ContentHash::from_bytes([0; 32]),
        authorization_digest: ContentHash::from_bytes([0; 32]),
        nonce: format!("plugin-state-nonce-{suffix}"),
        cancellation_id: command.cancellation_id.clone(),
        idempotency_key,
    };
    let request_hash = plugin_node_state_persistence_request_hash(&persistence)
        .map_err(PluginStateTurnError::Persistence)?;
    persistence.cancellation_target = plugin_invocation_cancellation_target(
        &command.session_id.to_string(),
        &command.identity.work.run_id,
        &command.identity.plugin_id,
        &command.plugin_version,
        &command.identity.invocation_id,
        &operation_id,
        command.identity.executor.executor_declaration_hash,
        request_hash,
    )
    .map_err(|_| PluginStateTurnError::Identity)?;
    let (action_digest, authorization_digest) = plugin_node_state_persistence_digests(&persistence)
        .map_err(PluginStateTurnError::Persistence)?;
    persistence.action_digest = action_digest;
    persistence.authorization_digest = authorization_digest;
    Ok(persistence)
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b':' | b'-')
        })
}

const fn map_scope(scope: PersistenceStateScope) -> crate::session::PluginNodeStateScope {
    match scope {
        PersistenceStateScope::Invocation => crate::session::PluginNodeStateScope::Invocation,
        PersistenceStateScope::ModelCall => crate::session::PluginNodeStateScope::ModelCall,
        PersistenceStateScope::Turn => crate::session::PluginNodeStateScope::Turn,
        PersistenceStateScope::Session => crate::session::PluginNodeStateScope::Session,
        PersistenceStateScope::Project => crate::session::PluginNodeStateScope::Project,
        PersistenceStateScope::User => crate::session::PluginNodeStateScope::User,
        PersistenceStateScope::Runtime => crate::session::PluginNodeStateScope::Runtime,
    }
}

const fn state_scope_name(scope: PersistenceStateScope) -> &'static str {
    match scope {
        PersistenceStateScope::Invocation => "invocation",
        PersistenceStateScope::ModelCall => "model_call",
        PersistenceStateScope::Turn => "turn",
        PersistenceStateScope::Session => "session",
        PersistenceStateScope::Project => "project",
        PersistenceStateScope::User => "user",
        PersistenceStateScope::Runtime => "runtime",
    }
}

fn result_from_event(
    event: &crate::session::PluginNodeStatePreservedEvent,
    status: PluginStateTurnStatus,
) -> PreservePluginNodeStateResult {
    PreservePluginNodeStateResult {
        generation: event.generation,
        state_hash: event.state_hash,
        terminal_receipt_id: event.terminal_receipt_id.clone(),
        status,
    }
}

fn classify_state_persistence_error(
    command: &PersistPluginNodeStateCommand,
    error: &PluginNodeStatePersistenceError,
) -> Result<(&'static str, bool), PluginStateTurnError> {
    let classified = match error {
        PluginNodeStatePersistenceError::InvalidCommand => ("state_command_invalid", false),
        PluginNodeStatePersistenceError::InvalidState => ("state_value_invalid", false),
        PluginNodeStatePersistenceError::InvalidDigest => ("state_digest_invalid", false),
        PluginNodeStatePersistenceError::InvalidReceipt => ("state_receipt_invalid", true),
        PluginNodeStatePersistenceError::Unsupported => ("state_persistence_unsupported", false),
        PluginNodeStatePersistenceError::UnsupportedScope => ("state_scope_unsupported", false),
        PluginNodeStatePersistenceError::StaleGeneration => ("state_generation_stale", false),
        PluginNodeStatePersistenceError::Conflict => ("state_conflict", false),
        PluginNodeStatePersistenceError::Cancelled => ("state_cancelled", true),
        PluginNodeStatePersistenceError::Ambiguous {
            plugin_id,
            invocation_id,
            idempotency_key,
        } => {
            if plugin_id != &command.plugin_id
                || invocation_id != &command.invocation_id
                || idempotency_key != &command.idempotency_key
            {
                return Err(PluginStateTurnError::InvocationSubstitution);
            }
            ("state_persistence_ambiguous", true)
        }
        PluginNodeStatePersistenceError::Data(error) => match error {
            PluginDataError::Invalid => ("state_boundary_invalid", true),
            PluginDataError::Unavailable => ("state_boundary_unavailable", true),
            PluginDataError::Inactive => ("state_plugin_inactive", false),
            PluginDataError::Rejected { .. } => ("state_boundary_rejected", false),
            PluginDataError::Ambiguous { .. }
            | PluginDataError::AmbiguousStatePersistence { .. }
            | PluginDataError::AmbiguousStateRead { .. }
            | PluginDataError::AmbiguousContextTransform { .. }
            | PluginDataError::AmbiguousMemoryWrite { .. } => ("state_boundary_ambiguous", true),
            PluginDataError::MemoryOperationUnsupported
            | PluginDataError::StatePersistenceUnsupported => {
                ("state_persistence_unsupported", false)
            }
            PluginDataError::StateReadUnsupported => ("state_read_unsupported", false),
            PluginDataError::UnsupportedStateScope => ("state_scope_unsupported", false),
            PluginDataError::StaleStateGeneration => ("state_generation_stale", false),
            PluginDataError::StateConflict => ("state_conflict", false),
            PluginDataError::Cancelled => ("state_cancelled", true),
        },
    };
    Ok(classified)
}

fn terminal_failure_error(failed: &PluginNodeStatePersistenceFailedEvent) -> PluginStateTurnError {
    PluginStateTurnError::TerminalFailure {
        code: failed.code.clone(),
        ambiguous: failed.ambiguous,
        idempotency_key: failed.idempotency_key.clone(),
    }
}

fn seal_event(
    head: &PluginStateTurnHead,
    sequence: Sequence,
    identity: PluginStateTurnEventIdentity,
    preserved: crate::session::PluginNodeStatePreservedEvent,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, PluginStateTurnError> {
    let parent_graph_node_id = Some(preserved.identity.work.node_id.clone());
    let payload = RuntimeCommittedEvent::PluginNodeStatePreserved(Box::new(preserved));
    EventEnvelope::seal(
        EventMetadata {
            event_id: identity.event_id,
            scope: EventScope::Session(head.state.id),
            sequence,
            timestamp: identity.timestamp,
            event_type: payload.event_type().to_owned(),
            event_version: Version::new(1, 0),
            correlation_id: identity.correlation_id,
            causation_id: CausationId::from_uuid(head.last_event_id.into_uuid()),
            parent_graph_node_id,
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
    .map_err(|_| PluginStateTurnError::Event)
}

fn seal_failure_event(
    head: &PluginStateTurnHead,
    sequence: Sequence,
    identity: PluginStateTurnEventIdentity,
    failed: PluginNodeStatePersistenceFailedEvent,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, PluginStateTurnError> {
    let parent_graph_node_id = Some(failed.identity.work.node_id.clone());
    let payload = RuntimeCommittedEvent::PluginNodeStatePersistenceFailed(Box::new(failed));
    EventEnvelope::seal(
        EventMetadata {
            event_id: identity.event_id,
            scope: EventScope::Session(head.state.id),
            sequence,
            timestamp: identity.timestamp,
            event_type: payload.event_type().to_owned(),
            event_version: Version::new(1, 0),
            correlation_id: identity.correlation_id,
            causation_id: CausationId::from_uuid(head.last_event_id.into_uuid()),
            parent_graph_node_id,
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
    .map_err(|_| PluginStateTurnError::Event)
}

/// Live plugin-state coordination failure.
#[derive(Debug, Error)]
pub enum PluginStateTurnError {
    /// The command selected a scope without an exact canonical key.
    #[error("plugin state scope is unsupported by the session coordinator")]
    UnsupportedScope,
    /// Turn cancellation identity is empty or outside its canonical bound.
    #[error("plugin state cancellation identity is invalid")]
    InvalidCancellation,
    /// Replayed session differs from the requested session.
    #[error("plugin state session identity does not match")]
    SessionMismatch,
    /// Invocation identity differs from canonical replay.
    #[error("plugin state invocation identity was substituted")]
    InvocationSubstitution,
    /// Outcome application is incomplete or differs from the validated cut.
    #[error("plugin state outcome application is not ready")]
    InvalidApplication,
    /// A different terminal state receipt occupies the canonical cut.
    #[error("plugin state terminal receipt conflicts with canonical replay")]
    ConflictingReceipt,
    /// Stable deterministic operation identity could not be encoded.
    #[error("plugin state operation identity could not be constructed")]
    Identity,
    /// The external effect may have crossed its boundary. It is not retried.
    #[error(
        "plugin state persistence is ambiguous for `{plugin_id}` invocation `{invocation_id}` idempotency `{idempotency_key}`"
    )]
    Ambiguous {
        /// Exact plugin identity.
        plugin_id: String,
        /// Exact invocation identity.
        invocation_id: String,
        /// Exact deterministic idempotency identity.
        idempotency_key: String,
    },
    /// A canonical terminal failure suppresses every future storage call.
    #[error(
        "plugin state persistence terminally failed with `{code}` (ambiguous={ambiguous}) idempotency `{idempotency_key}`"
    )]
    TerminalFailure {
        /// Stable redacted classification.
        code: String,
        /// Whether the external write may have occurred.
        ambiguous: bool,
        /// Exact deterministic state-operation identity.
        idempotency_key: String,
    },
    /// Journal sequence overflow.
    #[error("plugin state journal sequence overflow")]
    Sequence,
    /// Event envelope construction failed.
    #[error("plugin state canonical event construction failed")]
    Event,
    /// Journal conflicts exceeded the bounded retry budget.
    #[error("plugin state journal conflict retry limit exceeded")]
    AppendConflictLimit,
    /// Plugin persistence boundary rejected the command or receipt.
    #[error("plugin state persistence failed: {0}")]
    Persistence(PluginNodeStatePersistenceError),
    /// Canonical replay/preparation/reducer validation failed.
    #[error("plugin state canonical validation failed: {0}")]
    Reducer(#[from] SessionReducerError),
    /// Canonical journal boundary failed.
    #[error(transparent)]
    Journal(#[from] PluginStateTurnJournalError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use agentmod_event_model::{EventMetadata, EventOrigin};
    use agentmod_primitives::{CausationId, CorrelationId};
    use agentmod_session_style_sdk::{BuiltInStyle, CompiledSessionStyle};
    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::{
        plugin::{
            LoadedPluginNodeState, PluginNodeStatePersistenceReceipt, PluginNodeStateReadReceipt,
            plugin_node_state_read_receipt_digest,
        },
        session::{
            CanonicalPluginNodeOutcomeProposal, PluginNodeBudgetChargedEvent,
            PluginNodeBudgetUsage, PluginNodeInvocationAuthorizedEvent,
            PluginNodeInvocationCompletedEvent, PluginNodeInvocationDispatchedEvent,
            PluginNodeInvocationProposedEvent, PluginNodeOutcomeValidatedEvent,
            SessionCreatedEvent, SessionNodeExecutorBoundary, SessionNodeExecutorSource,
            SessionStyleBinding, StyleExecutionContract, StyleExecutionInitializedEvent,
            StyleNodeEnteredEvent, plugin_node_budget_usage_hash,
            plugin_node_outcome_proposal_hash, plugin_node_outcome_validation_hash,
            plugin_node_value_hash, replay,
        },
        style_executor::tests::binding,
    };

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(1))
    }

    fn event_id(sequence: u64) -> EventId {
        EventId::from_uuid(Uuid::from_u128(100 + u128::from(sequence)))
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: event_id(sequence),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(2)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(3)),
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
        .expect("event")
    }

    fn execution_contract(
        binding: &SessionStyleBinding,
        graph: &agentmod_graph_engine::ExecutableGraph,
    ) -> StyleExecutionContract {
        let plan = binding.execution_plan.as_ref().expect("plan");
        StyleExecutionContract {
            style_binding_hash: ContentHash::digest(&serde_json::to_vec(binding).expect("binding")),
            execution_plan_hash: binding.execution_plan_hash.expect("plan hash"),
            registry_hash: plan.registry_hash,
            node_executors: plan.nodes.clone(),
            initial_node_id: graph.nodes[graph.entry_index].id.clone(),
            initial_variables_json: String::from("{}"),
            invocation_provider: Some(String::from("mock")),
            invocation_model: Some(String::from("mock-model")),
            invocation_options_json: None,
            initial_budgets: binding.budgets,
            run_id: format!("style-run:{}", session_id()),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture constructs every canonical plugin application cut explicitly"
    )]
    fn ready_fixture() -> (
        SessionState,
        PluginNodeInvocationIdentity,
        ContentHash,
        serde_json::Value,
    ) {
        let mut style_binding = binding(BuiltInStyle::PersistentChat);
        let compiled: CompiledSessionStyle =
            serde_json::from_str(&style_binding.compiled_style_json).expect("compiled");
        let entry_node_id = compiled.graph.nodes[compiled.graph.entry_index].id.clone();
        let plan = style_binding.execution_plan.as_mut().expect("plan");
        let resolution = plan
            .nodes
            .iter_mut()
            .find(|resolution| resolution.node_id == entry_node_id)
            .expect("resolution");
        resolution.executor_id = String::from("fixture.plugin-state");
        resolution.executor_version = String::from("2.1.0");
        resolution.source = SessionNodeExecutorSource::Plugin {
            plugin_id: String::from("fixture.plugin-state"),
        };
        resolution.boundary = SessionNodeExecutorBoundary::PluginHost;
        resolution.executor_declaration_hash = ContentHash::digest(b"fixture.plugin-state@2.1.0");
        let resolution = resolution.clone();
        style_binding.execution_plan_hash = Some(ContentHash::digest(
            &serde_json::to_vec(plan).expect("plan serialization"),
        ));
        let contract = execution_contract(&style_binding, &compiled.graph);
        let invocation_digest = ContentHash::digest(b"plugin state invocation");
        let identity = PluginNodeInvocationIdentity {
            work: crate::node_execution::NodeWorkIdentity {
                run_id: contract.run_id.clone(),
                node_id: entry_node_id.clone(),
                branch_path: Vec::new(),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            },
            executor: resolution.clone(),
            configuration_hash: resolution.adapter_configuration_reference,
            plugin_id: String::from("fixture.plugin-state"),
            invocation_id: format!("plugin-node:{}", invocation_digest.to_hex()),
            invocation_digest,
            input_hash: ContentHash::digest(b"input"),
            readable_state_hash: ContentHash::digest(b"readable state"),
            causation_event_id: event_id(3),
        };
        let state_value = serde_json::json!({"cursor": "next"});
        let proposal = CanonicalPluginNodeOutcomeProposal {
            output: serde_json::json!({"answer": 42}),
            output_hash: plugin_node_value_hash(&serde_json::json!({"answer": 42}))
                .expect("output hash"),
            preserved_state: state_value.clone(),
            preserved_state_hash: plugin_node_value_hash(&state_value).expect("state hash"),
            proposed_actions: Vec::new(),
            proposed_actions_hash: crate::session::plugin_node_actions_hash(&[])
                .expect("actions hash"),
        };
        let usage = PluginNodeBudgetUsage {
            steps: 1,
            tokens: 0,
            cost_micros: 0,
            duration_ms: 0,
        };
        let mut validated = PluginNodeOutcomeValidatedEvent {
            identity: identity.clone(),
            prior_event_id: event_id(7),
            proposal_hash: plugin_node_outcome_proposal_hash(&proposal).expect("proposal hash"),
            variable_receipt_hashes: Vec::new(),
            artifact_hashes: Vec::new(),
            preserved_state_hash: proposal.preserved_state_hash,
            action_hashes: Vec::new(),
            budget_usage_hash: plugin_node_budget_usage_hash(&usage).expect("budget hash"),
            transition_hash: None,
            validation_hash: ContentHash::from_bytes([0; 32]),
        };
        validated.validation_hash =
            plugin_node_outcome_validation_hash(&validated).expect("validation hash");
        let validation_hash = validated.validation_hash;
        let events = vec![
            envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: style_binding.id.clone(),
                    style_binding: Some(Box::new(style_binding)),
                }),
            ),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(compiled.graph),
                        input_reference: None,
                        execution_contract: Some(Box::new(contract)),
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: entry_node_id,
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::PluginNodeInvocationProposed(Box::new(
                    PluginNodeInvocationProposedEvent {
                        identity: identity.clone(),
                    },
                )),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::PluginNodeInvocationAuthorized(Box::new(
                    PluginNodeInvocationAuthorizedEvent {
                        identity: identity.clone(),
                        prior_event_id: event_id(4),
                        authorization_digest: ContentHash::digest(b"invocation authorization"),
                    },
                )),
            ),
            envelope(
                6,
                RuntimeCommittedEvent::PluginNodeInvocationDispatched(Box::new(
                    PluginNodeInvocationDispatchedEvent {
                        identity: identity.clone(),
                        prior_event_id: event_id(5),
                        authorization_digest: ContentHash::digest(b"invocation authorization"),
                        dispatch_digest: ContentHash::digest(b"invocation dispatch"),
                    },
                )),
            ),
            envelope(
                7,
                RuntimeCommittedEvent::PluginNodeInvocationCompleted(Box::new(
                    PluginNodeInvocationCompletedEvent {
                        identity: identity.clone(),
                        prior_event_id: event_id(6),
                        proposal,
                        attempts: 1,
                    },
                )),
            ),
            envelope(
                8,
                RuntimeCommittedEvent::PluginNodeOutcomeValidated(Box::new(validated.clone())),
            ),
            envelope(
                9,
                RuntimeCommittedEvent::PluginNodeBudgetCharged(Box::new(
                    PluginNodeBudgetChargedEvent {
                        identity: identity.clone(),
                        validation_hash,
                        prior_event_id: event_id(8),
                        usage,
                        usage_hash: validated.budget_usage_hash,
                    },
                )),
            ),
        ];
        (
            replay(&events).expect("ready replay"),
            identity,
            validation_hash,
            state_value,
        )
    }

    #[derive(Clone, Copy)]
    enum AppendBehavior {
        Append,
        Conflict,
        Error,
        CommitThenError,
    }

    #[derive(Clone)]
    struct MockJournal {
        inner: Arc<Mutex<MockJournalState>>,
    }

    struct MockJournalState {
        state: SessionState,
        last_event_id: EventId,
        next_identity: u128,
        behaviors: VecDeque<AppendBehavior>,
        append_calls: usize,
    }

    impl MockJournal {
        fn new(state: SessionState, behaviors: impl IntoIterator<Item = AppendBehavior>) -> Self {
            let last_event_id = event_id(state.last_sequence.get());
            Self {
                inner: Arc::new(Mutex::new(MockJournalState {
                    state,
                    last_event_id,
                    next_identity: 1_000,
                    behaviors: behaviors.into_iter().collect(),
                    append_calls: 0,
                })),
            }
        }

        fn append_calls(&self) -> usize {
            self.inner.lock().expect("journal").append_calls
        }
    }

    impl PluginStateTurnJournal for MockJournal {
        fn load(&self) -> Result<PluginStateTurnHead, PluginStateTurnJournalError> {
            let state = self.inner.lock().expect("journal");
            Ok(PluginStateTurnHead {
                state: state.state.clone(),
                last_event_id: state.last_event_id,
            })
        }

        fn allocate_identity(
            &self,
        ) -> Result<PluginStateTurnEventIdentity, PluginStateTurnJournalError> {
            let mut state = self.inner.lock().expect("journal");
            let value = state.next_identity;
            state.next_identity += 1;
            Ok(PluginStateTurnEventIdentity {
                event_id: EventId::from_uuid(Uuid::from_u128(value)),
                timestamp: TimestampMillis::new(1_700_000_000_100),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(2)),
            })
        }

        fn append(
            &self,
            expected: PluginStateTurnAppendPosition,
            event: EventEnvelope<RuntimeCommittedEvent>,
        ) -> Result<PluginStateTurnAppendOutcome, PluginStateTurnJournalError> {
            let mut state = self.inner.lock().expect("journal");
            state.append_calls += 1;
            assert_eq!(expected.sequence, state.state.last_sequence);
            assert_eq!(expected.event_id, state.last_event_id);
            match state
                .behaviors
                .pop_front()
                .unwrap_or(AppendBehavior::Append)
            {
                AppendBehavior::Append => {
                    state.state =
                        reduce(Some(state.state.clone()), &event).expect("append reduction");
                    state.last_event_id = event.metadata.event_id;
                    Ok(PluginStateTurnAppendOutcome::Appended)
                }
                AppendBehavior::Conflict => Ok(PluginStateTurnAppendOutcome::Conflict),
                AppendBehavior::Error => Err(journal_error("append_unavailable")),
                AppendBehavior::CommitThenError => {
                    state.state =
                        reduce(Some(state.state.clone()), &event).expect("append reduction");
                    state.last_event_id = event.metadata.event_id;
                    Err(journal_error("connection_lost_after_commit"))
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    enum PersistenceMode {
        Success,
        Ambiguous,
        SubstituteReceipt,
    }

    #[derive(Clone)]
    struct MockPersistence {
        mode: PersistenceMode,
        calls: Arc<AtomicUsize>,
        commands: Arc<Mutex<Vec<PersistPluginNodeStateCommand>>>,
    }

    impl MockPersistence {
        fn new(mode: PersistenceMode) -> Self {
            Self {
                mode,
                calls: Arc::new(AtomicUsize::new(0)),
                commands: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn commands(&self) -> Vec<PersistPluginNodeStateCommand> {
            self.commands.lock().expect("commands").clone()
        }
    }

    #[async_trait]
    impl PluginNodeStatePersistenceLogicPort for MockPersistence {
        async fn persist_plugin_node_state(
            &self,
            command: PersistPluginNodeStateCommand,
        ) -> Result<PluginNodeStatePersistenceReceipt, PluginNodeStatePersistenceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.commands
                .lock()
                .expect("commands")
                .push(command.clone());
            if matches!(self.mode, PersistenceMode::Ambiguous) {
                return Err(PluginNodeStatePersistenceError::Ambiguous {
                    plugin_id: command.plugin_id,
                    invocation_id: command.invocation_id,
                    idempotency_key: command.idempotency_key,
                });
            }
            let mut receipt = PluginNodeStatePersistenceReceipt {
                plugin_id: command.plugin_id,
                invocation_id: command.invocation_id,
                invocation_digest: command.invocation_digest,
                executor_id: command.executor_id,
                executor_version: command.executor_version,
                executor_declaration_hash: command.executor_declaration_hash,
                state_scope: command.state_scope,
                prior_generation: command.prior_generation,
                generation: command.prior_generation + 1,
                state_hash: command.state_hash,
                action_digest: command.action_digest,
                authorization_digest: command.authorization_digest,
                idempotency_key: command.idempotency_key,
                receipt_id: String::new(),
                receipt_digest: ContentHash::from_bytes([0; 32]),
                replayed: false,
            };
            receipt.receipt_id = format!(
                "plugin-state-receipt:{}",
                ContentHash::digest(receipt.idempotency_key.as_bytes()).to_hex()
            );
            if matches!(self.mode, PersistenceMode::SubstituteReceipt) {
                receipt.executor_version = String::from("9.9.9");
            }
            receipt.receipt_digest =
                crate::plugin::plugin_node_state_persistence_receipt_digest(&receipt)?;
            Ok(receipt)
        }
    }

    #[derive(Clone, Copy)]
    enum ReadMode {
        Success,
        Ambiguous,
        Stale,
        Unavailable,
        SubstituteState,
    }

    #[derive(Clone)]
    struct MockReader {
        mode: ReadMode,
        value: serde_json::Value,
        calls: Arc<AtomicUsize>,
        commands: Arc<Mutex<Vec<LoadPluginNodeStateCommand>>>,
    }

    impl MockReader {
        fn new(mode: ReadMode, value: serde_json::Value) -> Self {
            Self {
                mode,
                value,
                calls: Arc::new(AtomicUsize::new(0)),
                commands: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl PluginNodeStateReadLogicPort for MockReader {
        async fn load_plugin_node_state(
            &self,
            command: LoadPluginNodeStateCommand,
        ) -> Result<LoadedPluginNodeState, PluginNodeStateReadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.commands
                .lock()
                .expect("read commands")
                .push(command.clone());
            if matches!(self.mode, ReadMode::Ambiguous) {
                return Err(PluginNodeStateReadError::Ambiguous {
                    plugin_id: command.plugin_id,
                    invocation_id: command.invocation_id,
                    idempotency_key: command.idempotency_key,
                });
            }
            if matches!(self.mode, ReadMode::Stale) {
                return Err(PluginNodeStateReadError::StaleGeneration);
            }
            if matches!(self.mode, ReadMode::Unavailable) {
                return Err(PluginNodeStateReadError::Unsupported);
            }
            let mut receipt = PluginNodeStateReadReceipt {
                plugin_id: command.plugin_id,
                invocation_id: command.invocation_id,
                invocation_digest: command.invocation_digest,
                executor_id: command.executor_id,
                executor_version: command.executor_version,
                executor_declaration_hash: command.executor_declaration_hash,
                state_scope: command.state_scope,
                generation: command.expected_generation,
                state_hash: command.expected_state_hash,
                action_digest: command.action_digest,
                authorization_digest: command.authorization_digest,
                idempotency_key: command.idempotency_key,
                receipt_id: String::from("plugin-state-read-receipt:fixture"),
                receipt_digest: ContentHash::from_bytes([0; 32]),
                replayed: false,
            };
            receipt.receipt_digest =
                plugin_node_state_read_receipt_digest(&receipt).expect("read receipt digest");
            let state = if matches!(self.mode, ReadMode::SubstituteState) {
                serde_json::json!({"cursor":"substituted"})
            } else {
                self.value.clone()
            };
            Ok(LoadedPluginNodeState { state, receipt })
        }
    }

    fn command(
        identity: PluginNodeInvocationIdentity,
        validation_hash: ContentHash,
        state: serde_json::Value,
    ) -> PreservePluginNodeStateCommand {
        PreservePluginNodeStateCommand {
            session_id: session_id(),
            identity,
            plugin_version: String::from("1.0.0"),
            plugin_configuration_reference: ContentHash::digest(b"plugin-configuration"),
            validation_hash,
            state,
            state_scope: PersistenceStateScope::Invocation,
            cancellation_id: String::from("turn-cancel:fixture"),
        }
    }

    #[tokio::test]
    async fn commits_once_and_duplicate_recovery_never_calls_plugin_host_again() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::Append]);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        let coordinator = PluginStateTurnCoordinator::new(journal.clone(), persistence.clone());
        let command = command(identity, validation_hash, value);

        let committed = coordinator.preserve(command.clone()).await.expect("commit");
        assert_eq!(committed.status, PluginStateTurnStatus::Committed);
        assert_eq!(persistence.calls(), 1);
        assert_eq!(journal.append_calls(), 1);

        let recovered = coordinator.preserve(command).await.expect("recover");
        assert_eq!(recovered.status, PluginStateTurnStatus::AlreadyCommitted);
        assert_eq!(recovered.terminal_receipt_id, committed.terminal_receipt_id);
        assert_eq!(persistence.calls(), 1);
        assert_eq!(journal.append_calls(), 1);
    }

    #[tokio::test]
    async fn session_scope_uses_the_replayed_generation_chain() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::Append]);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        let mut command = command(identity, validation_hash, value);
        command.state_scope = PersistenceStateScope::Session;

        let result = PluginStateTurnCoordinator::new(journal, persistence.clone())
            .preserve(command)
            .await
            .expect("session state");
        assert_eq!(result.generation, 1);
        let commands = persistence.commands();
        assert_eq!(commands[0].state_scope, PersistenceStateScope::Session);
        assert_eq!(commands[0].prior_generation, 0);
        assert_eq!(commands[0].prior_state_hash, None);
    }

    fn read_command(
        identity: PluginNodeInvocationIdentity,
        state_scope: PersistenceStateScope,
    ) -> LoadPriorPluginNodeStateCommand {
        LoadPriorPluginNodeStateCommand {
            session_id: session_id(),
            identity,
            plugin_version: String::from("1.0.0"),
            plugin_configuration_reference: ContentHash::digest(b"plugin-configuration"),
            state_scope,
            cancellation_id: String::from("turn-cancel:fixture"),
        }
    }

    #[tokio::test]
    async fn state_read_skips_invocation_and_empty_session_scopes() {
        let (state, identity, _validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, []);
        let reader = MockReader::new(ReadMode::Success, value);
        let coordinator = PluginStateReadTurnCoordinator::new(journal.clone(), reader.clone());
        assert_eq!(
            coordinator
                .load(read_command(
                    identity.clone(),
                    PersistenceStateScope::Invocation
                ))
                .await
                .expect("invocation scope"),
            PriorPluginNodeState::None
        );
        assert_eq!(
            coordinator
                .load(read_command(identity, PersistenceStateScope::Session))
                .await
                .expect("empty session scope"),
            PriorPluginNodeState::None
        );
        assert_eq!(reader.calls(), 0);
    }

    #[tokio::test]
    async fn state_read_loads_exact_session_predecessor_with_stable_identity() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::Append]);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        let mut preserve = command(identity.clone(), validation_hash, value.clone());
        preserve.state_scope = PersistenceStateScope::Session;
        PluginStateTurnCoordinator::new(journal.clone(), persistence)
            .preserve(preserve)
            .await
            .expect("canonical session state");

        let reader = MockReader::new(ReadMode::Success, value.clone());
        let coordinator = PluginStateReadTurnCoordinator::new(journal.clone(), reader.clone());
        let command = read_command(identity, PersistenceStateScope::Session);
        for _ in 0..2 {
            let loaded = coordinator.load(command.clone()).await.expect("exact read");
            assert_eq!(
                loaded,
                PriorPluginNodeState::Loaded {
                    generation: 1,
                    state_hash: plugin_node_state_value_hash(&value).expect("state hash"),
                    state: value.clone(),
                }
            );
            let debug = format!("{loaded:?}");
            assert!(debug.contains("[redacted]"));
            assert!(!debug.contains("cursor"));
        }
        let commands = reader.commands.lock().expect("read commands");
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0], commands[1]);
        assert_ne!(commands[0].action_digest, ContentHash::from_bytes([0; 32]));
        assert_ne!(
            commands[0].authorization_digest,
            ContentHash::from_bytes([0; 32])
        );
        assert_eq!(
            journal.append_calls(),
            1,
            "state reads must not append raw state or receipts to the journal"
        );
    }

    #[tokio::test]
    async fn state_read_rejects_substitution_ambiguity_and_unsupported_scope_without_retry() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::Append]);
        let mut preserve = command(identity.clone(), validation_hash, value.clone());
        preserve.state_scope = PersistenceStateScope::Session;
        PluginStateTurnCoordinator::new(
            journal.clone(),
            MockPersistence::new(PersistenceMode::Success),
        )
        .preserve(preserve)
        .await
        .expect("canonical session state");

        let substituted = MockReader::new(ReadMode::SubstituteState, value.clone());
        assert!(matches!(
            PluginStateReadTurnCoordinator::new(journal.clone(), substituted.clone())
                .load(read_command(
                    identity.clone(),
                    PersistenceStateScope::Session
                ))
                .await,
            Err(PluginStateReadTurnError::InvalidPrior)
        ));
        assert_eq!(substituted.calls(), 1);

        let ambiguous = MockReader::new(ReadMode::Ambiguous, value.clone());
        assert!(matches!(
            PluginStateReadTurnCoordinator::new(journal.clone(), ambiguous.clone())
                .load(read_command(
                    identity.clone(),
                    PersistenceStateScope::Session
                ))
                .await,
            Err(PluginStateReadTurnError::Read(
                PluginNodeStateReadError::Ambiguous { .. }
            ))
        ));
        assert_eq!(ambiguous.calls(), 1);

        for (mode, expected) in [
            (ReadMode::Stale, PluginNodeStateReadError::StaleGeneration),
            (ReadMode::Unavailable, PluginNodeStateReadError::Unsupported),
        ] {
            let reader = MockReader::new(mode, value.clone());
            let result = PluginStateReadTurnCoordinator::new(journal.clone(), reader.clone())
                .load(read_command(
                    identity.clone(),
                    PersistenceStateScope::Session,
                ))
                .await;
            assert!(
                matches!(result, Err(PluginStateReadTurnError::Read(error)) if error == expected)
            );
            assert_eq!(reader.calls(), 1);
        }

        let unsupported = MockReader::new(ReadMode::Success, value);
        for scope in [
            PersistenceStateScope::ModelCall,
            PersistenceStateScope::Turn,
            PersistenceStateScope::Project,
            PersistenceStateScope::User,
            PersistenceStateScope::Runtime,
        ] {
            assert!(matches!(
                PluginStateReadTurnCoordinator::new(journal.clone(), unsupported.clone())
                    .load(read_command(identity.clone(), scope))
                    .await,
                Err(PluginStateReadTurnError::UnsupportedScope)
            ));
        }
        assert_eq!(unsupported.calls(), 0);
    }

    #[tokio::test]
    async fn journal_conflict_reclassifies_retained_receipt_without_external_redispatch() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::Conflict, AppendBehavior::Append]);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        let result = PluginStateTurnCoordinator::new(journal.clone(), persistence.clone())
            .preserve(command(identity, validation_hash, value))
            .await
            .expect("commit after conflict");
        assert_eq!(result.status, PluginStateTurnStatus::Committed);
        assert_eq!(persistence.calls(), 1);
        assert_eq!(journal.append_calls(), 2);
    }

    #[tokio::test]
    async fn crash_after_canonical_append_recovers_without_external_redispatch() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::CommitThenError]);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        let coordinator = PluginStateTurnCoordinator::new(journal.clone(), persistence.clone());
        let command = command(identity, validation_hash, value);

        assert!(matches!(
            coordinator.preserve(command.clone()).await,
            Err(PluginStateTurnError::Journal(_))
        ));
        assert_eq!(persistence.calls(), 1);
        let recovered = coordinator
            .preserve(command)
            .await
            .expect("restart recovery");
        assert_eq!(recovered.status, PluginStateTurnStatus::AlreadyCommitted);
        assert_eq!(persistence.calls(), 1);
    }

    #[tokio::test]
    async fn preappend_crash_reconciles_only_the_same_idempotent_receipt() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::Error, AppendBehavior::Append]);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        let coordinator = PluginStateTurnCoordinator::new(journal, persistence.clone());
        let command = command(identity, validation_hash, value);

        assert!(matches!(
            coordinator.preserve(command.clone()).await,
            Err(PluginStateTurnError::Journal(_))
        ));
        let recovered = coordinator
            .preserve(command)
            .await
            .expect("receipt recovery");
        assert_eq!(recovered.status, PluginStateTurnStatus::Committed);
        let commands = persistence.commands();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0], commands[1]);
    }

    #[tokio::test]
    async fn ambiguous_external_persistence_is_canonical_and_never_retried() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, []);
        let persistence = MockPersistence::new(PersistenceMode::Ambiguous);
        let coordinator = PluginStateTurnCoordinator::new(journal.clone(), persistence.clone());
        let command = command(identity, validation_hash, value);
        let error = coordinator
            .preserve(command.clone())
            .await
            .expect_err("ambiguous");
        assert!(matches!(
            error,
            PluginStateTurnError::TerminalFailure {
                ambiguous: true,
                ..
            }
        ));
        assert_eq!(persistence.calls(), 1);
        assert_eq!(journal.append_calls(), 1);
        assert!(matches!(
            coordinator.preserve(command).await,
            Err(PluginStateTurnError::TerminalFailure {
                ambiguous: true,
                ..
            })
        ));
        assert_eq!(persistence.calls(), 1);
        assert_eq!(journal.append_calls(), 1);
    }

    #[tokio::test]
    async fn invalid_scope_and_invocation_substitution_fail_before_external_boundary() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, []);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        let coordinator = PluginStateTurnCoordinator::new(journal, persistence.clone());
        let mut unsupported = command(identity.clone(), validation_hash, value.clone());
        unsupported.state_scope = PersistenceStateScope::Project;
        assert!(matches!(
            coordinator.preserve(unsupported).await,
            Err(PluginStateTurnError::UnsupportedScope)
        ));

        let mut substituted = command(identity, validation_hash, value);
        substituted.identity.executor.executor_version = String::from("9.9.9");
        assert!(matches!(
            coordinator.preserve(substituted).await,
            Err(PluginStateTurnError::InvocationSubstitution)
        ));
        assert_eq!(persistence.calls(), 0);
    }

    #[tokio::test]
    async fn substituted_terminal_receipt_is_canonically_ambiguous_without_retry() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, []);
        let persistence = MockPersistence::new(PersistenceMode::SubstituteReceipt);
        let coordinator = PluginStateTurnCoordinator::new(journal.clone(), persistence.clone());
        let command = command(identity, validation_hash, value);
        let error = coordinator
            .preserve(command.clone())
            .await
            .expect_err("substituted receipt");
        assert!(matches!(
            error,
            PluginStateTurnError::TerminalFailure {
                ambiguous: true,
                ..
            }
        ));
        assert_eq!(persistence.calls(), 1);
        assert_eq!(journal.append_calls(), 1);
        assert!(matches!(
            coordinator.preserve(command).await,
            Err(PluginStateTurnError::TerminalFailure {
                ambiguous: true,
                ..
            })
        ));
        assert_eq!(persistence.calls(), 1);
        assert_eq!(journal.append_calls(), 1);
    }

    #[tokio::test]
    async fn cancellation_identity_changes_authorization_but_not_write_idempotency() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let state_hash = plugin_node_state_value_hash(&value).expect("state hash");
        let prior =
            derive_plugin_node_state_prior(&state, &identity, PersistenceStateScope::Invocation)
                .expect("prior");
        let first = command(identity.clone(), validation_hash, value.clone());
        let mut second = command(identity, validation_hash, value);
        second.cancellation_id = String::from("turn-cancel:replacement");
        let first =
            build_persistence_command(&first, state_hash, prior.generation, prior.state_hash)
                .expect("first command");
        let second =
            build_persistence_command(&second, state_hash, prior.generation, prior.state_hash)
                .expect("second command");

        assert_eq!(first.idempotency_key, second.idempotency_key);
        assert_eq!(first.nonce, second.nonce);
        assert_ne!(first.authorization_digest, second.authorization_digest);
        assert_ne!(first.cancellation_id, second.cancellation_id);
    }

    #[tokio::test]
    async fn deterministic_command_identity_survives_preappend_crash_retry() {
        let (state, identity, validation_hash, value) = ready_fixture();
        let journal = MockJournal::new(state, [AppendBehavior::Conflict, AppendBehavior::Append]);
        let persistence = MockPersistence::new(PersistenceMode::Success);
        PluginStateTurnCoordinator::new(journal, persistence.clone())
            .preserve(command(identity, validation_hash, value))
            .await
            .expect("preserve");
        let commands = persistence.commands();
        assert_eq!(commands.len(), 1);
        let command = &commands[0];
        assert!(command.nonce.starts_with("plugin-state-nonce-"));
        assert!(command.idempotency_key.starts_with("plugin-state-write-"));
        assert_eq!(command.cancellation_id, "turn-cancel:fixture");
    }
}
