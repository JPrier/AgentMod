//! Live journal coordination for canonical graph-variable payloads.
//!
//! The pure variable coordinator prepares typed payloads. This module owns the
//! journal ordering seam: it reloads replay before every append, reclassifies
//! the retained receipt, resolves artifacts, reducer-validates the sealed
//! event, and uses a head-bound compare-and-swap append.

use std::{collections::BTreeSet, path::PathBuf, str::FromStr};

use agentmod_event_model::{
    ArtifactReference, EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_graph_engine::ExecutableGraph;
use agentmod_primitives::{
    CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId, TimestampMillis, Version,
};
use agentmod_runtime_data::{
    artifact::{ArtifactDataPort, InspectArtifactDataRequest},
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
};
use serde_json::Value;
use thiserror::Error;

use crate::{
    canonical_variable_coordinator::{
        CanonicalVariableCoordinator, CanonicalVariableCoordinatorError, PreparedVariableEvent,
        VariableRecoveryDecision,
    },
    node_execution::NodeWorkIdentity,
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort,
    },
    session::{
        ParallelBranchControlState, RuntimeCommittedEvent, SessionReducerError, SessionState,
        StyleExecutionControlState, reduce,
    },
};

const MAX_BATCH_EVENTS: usize = 1_024;
const MAX_APPEND_RETRIES: usize = 32;

/// Exact replay cut loaded before one candidate append.
#[derive(Clone, Debug)]
pub struct VariableTurnHead {
    /// Pure canonical session projection.
    pub state: SessionState,
    /// Event identity at the exact journal head.
    pub last_event_id: EventId,
}

/// Runtime-allocated identity and time for one variable event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VariableTurnEventIdentity {
    /// Unique canonical event identity.
    pub event_id: EventId,
    /// Runtime-recorded timestamp.
    pub timestamp: TimestampMillis,
    /// Session/run correlation identity.
    pub correlation_id: CorrelationId,
}

/// Exact current head supplied to the append CAS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VariableTurnAppendPosition {
    /// Current canonical sequence.
    pub sequence: Sequence,
    /// Current canonical event identity.
    pub event_id: EventId,
}

/// Result of one durable append attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VariableTurnAppendOutcome {
    /// Event was durably appended at the expected head.
    Appended,
    /// Another writer changed the expected head; replay must be reloaded.
    Conflict,
}

/// Stable journal boundary failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("variable turn journal failed: {code}")]
pub struct VariableTurnJournalError {
    /// Bounded diagnostic code.
    pub code: String,
}

/// Narrow journal and artifact boundary for live canonical variables.
pub trait VariableTurnJournal: Send + Sync + 'static {
    /// Loads and purely replays the exact current session head.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when load or replay fails.
    fn load(&self) -> Result<VariableTurnHead, VariableTurnJournalError>;

    /// Allocates one runtime-owned identity and exact timestamp.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when identity allocation fails.
    fn allocate_identity(&self) -> Result<VariableTurnEventIdentity, VariableTurnJournalError>;

    /// Resolves every declared artifact to exact immutable metadata.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error for absent, substituted, or unbounded
    /// artifact references.
    fn resolve_artifacts(
        &self,
        declared: &BTreeSet<String>,
        expected: &[ArtifactReference],
    ) -> Result<Vec<ArtifactReference>, VariableTurnJournalError>;

    /// CAS-appends one sealed and reducer-validated event.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error for durable storage failure. A changed
    /// head is returned as [`VariableTurnAppendOutcome::Conflict`].
    fn append(
        &self,
        expected: VariableTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<VariableTurnAppendOutcome, VariableTurnJournalError>;
}

/// Production data-backed journal adapter for canonical variable turns.
///
/// The adapter binds one exact session and artifact namespace. It delegates
/// replay and typed persistence validation to [`SessionPersistenceLogic`],
/// while the final append is an atomic expected-head compare-and-swap in the
/// journal dependency.
#[derive(Clone, Debug)]
pub struct SessionVariableTurnJournal<D> {
    data: D,
    persistence: SessionPersistenceLogic<D>,
    session_id: SessionId,
    session_directory: PathBuf,
    artifact_store_root: PathBuf,
}

impl<D> SessionVariableTurnJournal<D>
where
    D: Clone,
{
    /// Binds the adapter to one immutable session journal and artifact store.
    #[must_use]
    pub fn new(
        data: D,
        session_id: SessionId,
        session_directory: PathBuf,
        artifact_store_root: PathBuf,
    ) -> Self {
        Self {
            persistence: SessionPersistenceLogic::new(data.clone()),
            data,
            session_id,
            session_directory,
            artifact_store_root,
        }
    }
}

impl<D> VariableTurnJournal for SessionVariableTurnJournal<D>
where
    D: Clone
        + Send
        + Sync
        + ArtifactDataPort
        + EventIdentityDataPort
        + JournalEventDataPort
        + 'static,
{
    fn load(&self) -> Result<VariableTurnHead, VariableTurnJournalError> {
        self.persistence
            .load_session(LoadSessionCommand {
                session_directory: self.session_directory.clone(),
                expected_session_id: self.session_id,
            })
            .map(|loaded| VariableTurnHead {
                state: loaded.state,
                last_event_id: loaded.last_event_id,
            })
            .map_err(|_| variable_journal_error("load_failed"))
    }

    fn allocate_identity(&self) -> Result<VariableTurnEventIdentity, VariableTurnJournalError> {
        self.data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map(|identity| VariableTurnEventIdentity {
                event_id: identity.event_id,
                timestamp: identity.timestamp,
                correlation_id: identity.correlation_id,
            })
            .map_err(|_| variable_journal_error("identity_unavailable"))
    }

    fn resolve_artifacts(
        &self,
        declared: &BTreeSet<String>,
        expected: &[ArtifactReference],
    ) -> Result<Vec<ArtifactReference>, VariableTurnJournalError> {
        let expected_ids = expected
            .iter()
            .map(|artifact| format!("artifact:{}", artifact.id.as_str()))
            .collect::<BTreeSet<_>>();
        if expected_ids.len() != expected.len() || &expected_ids != declared {
            return Err(variable_journal_error("artifact_set_mismatch"));
        }

        let mut ordered = expected.to_vec();
        ordered.sort_by(|left, right| left.id.cmp(&right.id));
        for artifact in &ordered {
            let portable_reference = format!("artifact:{}", artifact.id.as_str());
            let record = self
                .data
                .inspect_artifact(InspectArtifactDataRequest {
                    store_root: self.artifact_store_root.clone(),
                    artifact_reference: portable_reference.clone(),
                })
                .map_err(|_| variable_journal_error("artifact_unavailable"))?;
            let content_hash = ContentHash::from_str(&record.content_hash)
                .map_err(|_| variable_journal_error("artifact_metadata_invalid"))?;
            if record.artifact_id != artifact.id.as_str()
                || record.artifact_reference != portable_reference
                || content_hash != artifact.content_hash
            {
                return Err(variable_journal_error("artifact_substitution"));
            }
        }
        Ok(ordered)
    }

    fn append(
        &self,
        expected: VariableTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<VariableTurnAppendOutcome, VariableTurnJournalError> {
        let event_id = event.metadata.event_id;
        let sequence = event.metadata.sequence;
        if expected.sequence.checked_next().ok() != Some(sequence) {
            return Err(variable_journal_error("append_sequence_invalid"));
        }
        let outcome = self
            .persistence
            .compare_append_event(CompareAppendSessionEventCommand {
                session_directory: self.session_directory.clone(),
                expected_head_event_id: expected.event_id,
                event,
                durability: CommitDurability::Data,
            })
            .map_err(|_| variable_journal_error("append_failed"))?;
        match outcome {
            CompareAppendSessionEventResult::Appended(committed)
                if committed.event_id == event_id && committed.sequence == sequence =>
            {
                Ok(VariableTurnAppendOutcome::Appended)
            }
            CompareAppendSessionEventResult::Appended(_) => {
                Err(variable_journal_error("append_receipt_mismatch"))
            }
            CompareAppendSessionEventResult::Conflict => Ok(VariableTurnAppendOutcome::Conflict),
        }
    }
}

fn variable_journal_error(code: &'static str) -> VariableTurnJournalError {
    VariableTurnJournalError {
        code: code.to_owned(),
    }
}

/// TurnLogic-ready command for one staged variable-event batch.
#[derive(Clone, Debug)]
pub struct CommitVariableBatchCommand {
    /// Canonical session.
    pub session_id: SessionId,
    /// Immutable compiled graph.
    pub graph: ExecutableGraph,
    /// Work-bound payload receipts in deterministic commit order.
    pub events: Vec<PreparedVariableEvent>,
    /// Exact artifact metadata expected by any batch payload.
    pub artifacts: Vec<ArtifactReference>,
}

/// Result of committing or recovering one batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitVariableBatchResult {
    /// Canonical head after the final reload.
    pub last_sequence: Sequence,
    /// Newly appended payload count.
    pub committed: usize,
    /// Prefix/already-applied payload count.
    pub already_applied: usize,
}

/// Live journal coordinator ready for composition into `TurnLogic`.
pub struct VariableTurnCoordinator<J> {
    journal: J,
}

impl<J> VariableTurnCoordinator<J> {
    /// Creates a journal coordinator over an injected boundary.
    #[must_use]
    pub const fn new(journal: J) -> Self {
        Self { journal }
    }
}

impl<J> VariableTurnCoordinator<J>
where
    J: VariableTurnJournal,
{
    /// Commits a freshly prepared batch.
    ///
    /// This is also safe after an append conflict: every attempt reloads and
    /// reclassifies the exact receipt before allocating a new event identity.
    ///
    /// # Errors
    ///
    /// Fails closed on invalid active work, graph/replay drift, artifact
    /// substitution, conflicting receipts, exhausted CAS retries, reducer
    /// rejection, or durable journal failure.
    pub fn commit_batch(
        &self,
        command: &CommitVariableBatchCommand,
    ) -> Result<CommitVariableBatchResult, VariableTurnError> {
        self.commit_or_recover(command)
    }

    /// Recovers a batch after any crash prefix without redispatching.
    ///
    /// # Errors
    ///
    /// Returns the same fail-closed errors as [`Self::commit_batch`].
    pub fn recover_batch(
        &self,
        command: &CommitVariableBatchCommand,
    ) -> Result<CommitVariableBatchResult, VariableTurnError> {
        self.commit_or_recover(command)
    }

    /// Loads a fresh replay cut and returns the exact authorized transition
    /// environment for active work.
    ///
    /// # Errors
    ///
    /// Fails closed when the session, graph, run, active work, declaration
    /// reads, consumer identity, branch scope, or replay integrity differs.
    pub fn transition_environment(
        &self,
        session_id: SessionId,
        graph: &ExecutableGraph,
        work: &NodeWorkIdentity,
        required_variables: &BTreeSet<String>,
    ) -> Result<Value, VariableTurnError> {
        let head = self.journal.load()?;
        let replayed = validate_head(&head, session_id, graph, work)?;
        CanonicalVariableCoordinator::new(replayed, graph, work)?
            .transition_environment(required_variables)
            .map_err(VariableTurnError::Coordinator)
    }

    /// Loads a fresh replay cut and returns exact declared pre-execution inputs,
    /// omitting only an unassigned variable produced by the same node.
    ///
    /// # Errors
    ///
    /// Fails closed on session, graph, work, declaration, authorization,
    /// branch-scope, or replay mismatch.
    pub fn node_input_environment(
        &self,
        session_id: SessionId,
        graph: &ExecutableGraph,
        work: &NodeWorkIdentity,
        read_variables: &BTreeSet<String>,
        write_variables: &BTreeSet<String>,
    ) -> Result<Value, VariableTurnError> {
        let head = self.journal.load()?;
        let replayed = validate_head(&head, session_id, graph, work)?;
        CanonicalVariableCoordinator::new(replayed, graph, work)?
            .node_input_environment(read_variables, write_variables)
            .map_err(VariableTurnError::Coordinator)
    }

    fn commit_or_recover(
        &self,
        command: &CommitVariableBatchCommand,
    ) -> Result<CommitVariableBatchResult, VariableTurnError> {
        if command.events.is_empty() || command.events.len() > MAX_BATCH_EVENTS {
            return Err(VariableTurnError::InvalidBatch);
        }
        validate_batch_artifacts(command)?;
        let mut committed = 0;
        let mut already_applied = 0;
        for receipt in &command.events {
            match self.commit_receipt(command, receipt)? {
                ReceiptCommitOutcome::Committed => committed += 1,
                ReceiptCommitOutcome::AlreadyApplied => already_applied += 1,
            }
        }
        let head = self.journal.load()?;
        if head.state.id != command.session_id {
            return Err(VariableTurnError::SessionMismatch);
        }
        Ok(CommitVariableBatchResult {
            last_sequence: head.state.last_sequence,
            committed,
            already_applied,
        })
    }

    fn commit_receipt(
        &self,
        command: &CommitVariableBatchCommand,
        receipt: &PreparedVariableEvent,
    ) -> Result<ReceiptCommitOutcome, VariableTurnError> {
        let operation = receipt.operation()?;
        for _ in 0..MAX_APPEND_RETRIES {
            let head = self.journal.load()?;
            let replayed = validate_head(&head, command.session_id, &command.graph, &receipt.work)?;
            let coordinator =
                CanonicalVariableCoordinator::new(replayed, &command.graph, &receipt.work)?;
            let payload = match coordinator.recover(&operation, Some(receipt))? {
                VariableRecoveryDecision::CompleteFromReceipt(prepared) => prepared.payload,
                VariableRecoveryDecision::AlreadyApplied => {
                    return Ok(ReceiptCommitOutcome::AlreadyApplied);
                }
                VariableRecoveryDecision::SafeToCommit(_) | VariableRecoveryDecision::Conflict => {
                    return Err(VariableTurnError::ConflictingReceipt);
                }
            };
            let declared_artifacts = variable_artifacts(&payload)?;
            let expected_artifacts = command
                .artifacts
                .iter()
                .filter(|artifact| {
                    declared_artifacts.contains(&format!("artifact:{}", artifact.id.as_str()))
                })
                .cloned()
                .collect::<Vec<_>>();
            let artifacts = self
                .journal
                .resolve_artifacts(&declared_artifacts, &expected_artifacts)?;
            let identity = self.journal.allocate_identity()?;
            let sequence = head
                .state
                .last_sequence
                .checked_next()
                .map_err(|_| VariableTurnError::Sequence)?;
            let event =
                seal_variable_event(&head, sequence, identity, receipt, payload, artifacts)?;
            reduce(Some(head.state.clone()), &event)?;
            match self.journal.append(
                VariableTurnAppendPosition {
                    sequence: head.state.last_sequence,
                    event_id: head.last_event_id,
                },
                event,
            )? {
                VariableTurnAppendOutcome::Appended => {
                    return Ok(ReceiptCommitOutcome::Committed);
                }
                VariableTurnAppendOutcome::Conflict => {}
            }
        }
        Err(VariableTurnError::AppendConflictLimit)
    }
}

fn validate_batch_artifacts(command: &CommitVariableBatchCommand) -> Result<(), VariableTurnError> {
    let mut declared = BTreeSet::new();
    for event in &command.events {
        declared.extend(variable_artifacts(&event.payload)?);
    }
    let supplied = command
        .artifacts
        .iter()
        .map(|artifact| format!("artifact:{}", artifact.id.as_str()))
        .collect::<BTreeSet<_>>();
    if supplied.len() != command.artifacts.len() || supplied != declared {
        return Err(VariableTurnError::ArtifactSetMismatch);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiptCommitOutcome {
    Committed,
    AlreadyApplied,
}

fn validate_head<'a>(
    head: &'a VariableTurnHead,
    session_id: SessionId,
    graph: &ExecutableGraph,
    work: &NodeWorkIdentity,
) -> Result<&'a crate::canonical_variables::CanonicalVariableEventReducer, VariableTurnError> {
    if head.state.id != session_id {
        return Err(VariableTurnError::SessionMismatch);
    }
    let execution = head
        .state
        .style_execution
        .as_ref()
        .ok_or(VariableTurnError::MissingExecution)?;
    if &*execution.graph != graph {
        return Err(VariableTurnError::GraphMismatch);
    }
    let replayed = execution
        .canonical_variables
        .as_deref()
        .ok_or(VariableTurnError::MissingVariables)?;
    if replayed.run_id() != work.run_id {
        return Err(VariableTurnError::RunMismatch);
    }
    validate_active_work(execution, work)?;
    Ok(replayed)
}

fn validate_active_work(
    execution: &crate::session::StyleExecutionState,
    work: &NodeWorkIdentity,
) -> Result<(), VariableTurnError> {
    if work.node_id == "runtime" && work.branch_path.is_empty() {
        return match &execution.control {
            StyleExecutionControlState::ReadyForEntry(_) if execution.active_node.is_none() => {
                Ok(())
            }
            _ => Err(VariableTurnError::InactiveWork),
        };
    }
    if work.branch_path.is_empty() {
        return match &execution.control {
            StyleExecutionControlState::Active(entered)
                if entered.node_id == work.node_id
                    && entered.attempt == work.attempt
                    && entered.loop_iteration == work.loop_iteration
                    && entered.step == work.step =>
            {
                Ok(())
            }
            _ => Err(VariableTurnError::InactiveWork),
        };
    }
    let active = execution
        .parallel_executions
        .values()
        .flat_map(|parallel| parallel.branches.values())
        .any(|branch| {
            matches!(
                &branch.control,
                ParallelBranchControlState::Active(entered) if entered.work == *work
            )
        });
    if active {
        Ok(())
    } else {
        Err(VariableTurnError::InactiveWork)
    }
}

fn variable_artifacts(
    payload: &RuntimeCommittedEvent,
) -> Result<BTreeSet<String>, VariableTurnError> {
    payload
        .canonical_variable_event()
        .map(|event| event.binding().artifact_references.clone())
        .ok_or(VariableTurnError::InvalidPayload)
}

fn seal_variable_event(
    head: &VariableTurnHead,
    sequence: Sequence,
    identity: VariableTurnEventIdentity,
    receipt: &PreparedVariableEvent,
    payload: RuntimeCommittedEvent,
    artifacts: Vec<ArtifactReference>,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, VariableTurnError> {
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
            parent_graph_node_id: Some(receipt.work.node_id.clone()),
            origin: EventOrigin {
                subsystem: String::from("runtime"),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts,
            classification: EventClassification::Committed,
        },
        payload,
    )
    .map_err(|_| VariableTurnError::Event)
}

/// Live canonical-variable journal coordination failure.
#[derive(Debug, Error)]
pub enum VariableTurnError {
    /// Batch is empty or exceeds the canonical bound.
    #[error("canonical variable batch is invalid")]
    InvalidBatch,
    /// Loaded session differs from the command.
    #[error("canonical variable session does not match")]
    SessionMismatch,
    /// Style execution projection is absent.
    #[error("canonical variable style execution is unavailable")]
    MissingExecution,
    /// Loaded compiled graph differs from the immutable command.
    #[error("canonical variable compiled graph does not match")]
    GraphMismatch,
    /// Canonical variable replay projection is absent.
    #[error("canonical variable replay is unavailable")]
    MissingVariables,
    /// Work run differs from the immutable execution run.
    #[error("canonical variable work run does not match")]
    RunMismatch,
    /// Exact root or branch work is not currently active.
    #[error("canonical variable work is not active")]
    InactiveWork,
    /// Receipt payload is not a canonical variable event.
    #[error("canonical variable payload is invalid")]
    InvalidPayload,
    /// Supplied artifact set is not exactly the batch's declared set.
    #[error("canonical variable artifact set does not match the batch")]
    ArtifactSetMismatch,
    /// Receipt conflicts with fresh replay or intended operation.
    #[error("canonical variable receipt conflicts with replay")]
    ConflictingReceipt,
    /// Sequence allocation overflowed.
    #[error("canonical variable sequence overflow")]
    Sequence,
    /// Canonical event sealing failed.
    #[error("canonical variable event sealing failed")]
    Event,
    /// CAS conflicts exceeded the bounded retry count.
    #[error("canonical variable append conflict retry limit exceeded")]
    AppendConflictLimit,
    /// Pure coordinator rejected graph/work/replay.
    #[error("canonical variable coordinator failed: {0}")]
    Coordinator(#[from] CanonicalVariableCoordinatorError),
    /// Pure session reducer rejected the sealed event.
    #[error("canonical variable reducer rejected event: {0}")]
    Reducer(#[from] SessionReducerError),
    /// Journal or artifact boundary failed.
    #[error(transparent)]
    Journal(#[from] VariableTurnJournalError),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};

    use agentmod_event_model::{
        ArtifactIdentifier, ArtifactReference, EventClassification, EventEnvelope, EventMetadata,
        EventOrigin, EventScope,
    };
    use agentmod_graph_engine::{
        ExecutableGraph, ExecutableNode, GraphBudget, GraphCacheKey, GraphDeclarations, NodeKind,
        SecurityClassification, VariableDeclaration, VariableMergePolicy, VariableMutability,
        VariableScope, VariableValueType,
    };
    use agentmod_primitives::{
        CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId, TimestampMillis,
        Version,
    };
    use agentmod_runtime_data::{
        artifact::{
            ArtifactDataPort, ArtifactRetentionRecord, ArtifactSecurityRecord,
            PersistArtifactDataRequest,
        },
        journal::{JournalEventDataPort, ScanEventsDataRequest},
        local::{LocalRuntimeDataPort, local_runtime_data_with_artifacts},
    };
    use uuid::Uuid;

    use super::{
        CommitVariableBatchCommand, SessionVariableTurnJournal, VariableTurnAppendOutcome,
        VariableTurnAppendPosition, VariableTurnCoordinator, VariableTurnEventIdentity,
        VariableTurnHead, VariableTurnJournal, VariableTurnJournalError,
    };
    use crate::{
        canonical_variable_coordinator::{
            CanonicalVariableCoordinator, CoordinatedVariableOperation, PreparedVariableEvent,
        },
        canonical_variables::{
            BranchVariableValue, CanonicalVariableEventReducer, CanonicalVariableValue,
            VariableEnvironmentLimits,
        },
        conversation::ConversationState,
        node_execution::NodeWorkIdentity,
        persistence::{
            CommitDurability, CommitSessionEventCommand, SessionPersistenceLogic,
            SessionPersistenceLogicPort,
        },
        session::{
            PlannerWorkerState, PluginExecutionState, RuntimeCommittedEvent, SessionCreatedEvent,
            SessionLifecycle, SessionLifecycleChangedEvent, SessionState,
            StyleExecutionControlState, StyleExecutionState, StyleNodeEnteredEvent, reduce,
        },
    };

    #[derive(Clone)]
    struct MockJournal {
        inner: Arc<Mutex<MockState>>,
    }

    struct MockState {
        state: SessionState,
        last_event_id: EventId,
        next_identity: u128,
        append_count: usize,
        cut_before_next: bool,
        cut_after_append: Option<usize>,
        conflict_once: bool,
        artifacts: BTreeMap<String, ArtifactReference>,
        events: Vec<EventEnvelope<crate::session::RuntimeCommittedEvent>>,
    }

    impl MockJournal {
        fn new(state: SessionState) -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockState {
                    state,
                    last_event_id: EventId::from_uuid(Uuid::from_u128(10)),
                    next_identity: 100,
                    append_count: 0,
                    cut_before_next: false,
                    cut_after_append: None,
                    conflict_once: false,
                    artifacts: BTreeMap::new(),
                    events: Vec::new(),
                })),
            }
        }

        fn cut_before_next(&self) {
            self.inner.lock().expect("state").cut_before_next = true;
        }

        fn cut_after(&self, append_count: usize) {
            self.inner.lock().expect("state").cut_after_append = Some(append_count);
        }

        fn conflict_once(&self) {
            self.inner.lock().expect("state").conflict_once = true;
        }

        fn register_artifact(&self, artifact: ArtifactReference) {
            self.inner
                .lock()
                .expect("state")
                .artifacts
                .insert(artifact.id.as_str().to_owned(), artifact);
        }

        fn snapshot(&self) -> SessionState {
            self.inner.lock().expect("state").state.clone()
        }

        fn event_count(&self) -> usize {
            self.inner.lock().expect("state").events.len()
        }

        fn last_event(&self) -> EventEnvelope<crate::session::RuntimeCommittedEvent> {
            self.inner
                .lock()
                .expect("state")
                .events
                .last()
                .expect("event")
                .clone()
        }
    }

    impl VariableTurnJournal for MockJournal {
        fn load(&self) -> Result<VariableTurnHead, VariableTurnJournalError> {
            let state = self.inner.lock().expect("state");
            Ok(VariableTurnHead {
                state: state.state.clone(),
                last_event_id: state.last_event_id,
            })
        }

        fn allocate_identity(&self) -> Result<VariableTurnEventIdentity, VariableTurnJournalError> {
            let mut state = self.inner.lock().expect("state");
            state.next_identity += 1;
            Ok(VariableTurnEventIdentity {
                event_id: EventId::from_uuid(Uuid::from_u128(state.next_identity)),
                timestamp: TimestampMillis::new(
                    i64::try_from(state.next_identity).expect("timestamp"),
                ),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(1)),
            })
        }

        fn resolve_artifacts(
            &self,
            declared: &BTreeSet<String>,
            expected: &[ArtifactReference],
        ) -> Result<Vec<ArtifactReference>, VariableTurnJournalError> {
            let state = self.inner.lock().expect("state");
            if declared.len() != expected.len()
                || expected.iter().any(|artifact| {
                    !declared.contains(&format!("artifact:{}", artifact.id.as_str()))
                        || state.artifacts.get(artifact.id.as_str()) != Some(artifact)
                })
            {
                return Err(journal_error("artifact_mismatch"));
            }
            let mut resolved = expected.to_vec();
            resolved.sort_by(|left, right| left.id.cmp(&right.id));
            Ok(resolved)
        }

        fn append(
            &self,
            expected: VariableTurnAppendPosition,
            event: EventEnvelope<crate::session::RuntimeCommittedEvent>,
        ) -> Result<VariableTurnAppendOutcome, VariableTurnJournalError> {
            let mut state = self.inner.lock().expect("state");
            if state.conflict_once {
                state.conflict_once = false;
                return Ok(VariableTurnAppendOutcome::Conflict);
            }
            if state.cut_before_next {
                state.cut_before_next = false;
                return Err(journal_error("cut_before_append"));
            }
            if expected.sequence != state.state.last_sequence
                || expected.event_id != state.last_event_id
            {
                return Ok(VariableTurnAppendOutcome::Conflict);
            }
            state.state =
                reduce(Some(state.state.clone()), &event).map_err(|_| journal_error("reducer"))?;
            state.last_event_id = event.metadata.event_id;
            state.append_count += 1;
            state.events.push(event);
            if state.cut_after_append == Some(state.append_count) {
                state.cut_after_append = None;
                return Err(journal_error("cut_after_append"));
            }
            Ok(VariableTurnAppendOutcome::Appended)
        }
    }

    fn journal_error(code: &str) -> VariableTurnJournalError {
        VariableTurnJournalError {
            code: code.to_owned(),
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(1))
    }

    fn work() -> NodeWorkIdentity {
        NodeWorkIdentity {
            run_id: String::from("run-variable-turn"),
            node_id: String::from("compute"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        }
    }

    fn declaration(
        name: &str,
        value_type: VariableValueType,
        producer: &str,
        merge_policy: Option<VariableMergePolicy>,
    ) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            value_type,
            scope: VariableScope::Run,
            producer: producer.to_owned(),
            merge_contributors: BTreeSet::new(),
            consumers: BTreeSet::from([String::from("compute")]),
            mutability: VariableMutability::Mutable,
            merge_policy,
            max_size_bytes: 4_096,
            security_classification: SecurityClassification::Internal,
        }
    }

    fn graph() -> ExecutableGraph {
        let hash = ContentHash::digest(b"variable-turn");
        ExecutableGraph {
            format_version: 1,
            entry_index: 0,
            budget: GraphBudget {
                max_steps: 32,
                max_tokens: 1_024,
                max_cost_micros: 1_000,
                max_duration_ms: 60_000,
            },
            declarations: GraphDeclarations::default(),
            variables: vec![
                declaration(
                    "artifact",
                    VariableValueType::ArtifactReference,
                    "compute",
                    None,
                ),
                declaration("input", VariableValueType::Boolean, "runtime", None),
                declaration("one", VariableValueType::String, "compute", None),
                declaration(
                    "shared",
                    VariableValueType::List {
                        item_type: Box::new(VariableValueType::Integer),
                        max_items: 8,
                    },
                    "compute",
                    Some(VariableMergePolicy::Append),
                ),
                declaration("two", VariableValueType::Integer, "compute", None),
            ],
            nodes: vec![ExecutableNode {
                index: 0,
                id: String::from("compute"),
                kind: NodeKind::ConditionalBranch,
                configuration: None,
                condition: None,
                tool: None,
                provider: None,
                required_capabilities: BTreeSet::new(),
                read_scopes: BTreeSet::new(),
                write_scopes: BTreeSet::new(),
                read_variables: BTreeSet::from([String::from("input")]),
                write_variables: BTreeSet::from([
                    String::from("artifact"),
                    String::from("one"),
                    String::from("shared"),
                    String::from("two"),
                ]),
                retry_limit: 0,
                max_iterations: None,
            }],
            edges: Vec::new(),
            cache_key: GraphCacheKey {
                graph_content_hash: hash,
                plugin_set_hash: hash,
                capability_set_hash: hash,
                runtime_api_hash: hash,
                combined_hash: hash,
            },
        }
    }

    fn initial_reducer(graph: &ExecutableGraph) -> CanonicalVariableEventReducer {
        CanonicalVariableEventReducer::initialize(
            "run-variable-turn",
            VariableEnvironmentLimits::default(),
            graph.variables.clone(),
            [(String::from("input"), CanonicalVariableValue::Boolean(true))],
        )
        .expect("variables")
    }

    fn style_execution(
        graph: &ExecutableGraph,
        variables: CanonicalVariableEventReducer,
    ) -> StyleExecutionState {
        let entered = StyleNodeEnteredEvent {
            node_id: String::from("compute"),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        };
        StyleExecutionState {
            completed_turn_runs: Vec::new(),
            generic_model_invocations: BTreeMap::new(),
            graph: Box::new(graph.clone()),
            input_reference: None,
            execution_contract: None,
            canonical_variables: Some(Box::new(variables)),
            control: StyleExecutionControlState::Active(entered.clone()),
            active_node: Some(entered),
            active_node_entered_at: Some(Sequence::new(3).expect("sequence")),
            completed_nodes: Vec::new(),
            emitted_user_events: Vec::new(),
            graph_schedules: BTreeMap::new(),
            child_messages: BTreeMap::new(),
            plugin_node_invocations: BTreeMap::new(),
            parallel_executions: BTreeMap::new(),
            generic_joins: BTreeMap::new(),
            failed_nodes: Vec::new(),
            transitions: Vec::new(),
            termination_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            cost_micros: 0,
            cost_estimated: false,
            tokens_at_last_compaction: 0,
            context_boundaries: Vec::new(),
            latest_model_execution: None,
        }
    }

    fn session_state(
        graph: &ExecutableGraph,
        variables: CanonicalVariableEventReducer,
    ) -> SessionState {
        SessionState {
            id: session_id(),
            workspace: String::from("fixture"),
            style: String::from("user-variable"),
            style_binding: None,
            style_execution: Some(style_execution(graph, variables)),
            ancestry: None,
            child_origin: None,
            workspace_lease: None,
            lifecycle: SessionLifecycle::Active,
            successful_session_completion: None,
            successful_iteration_completions: Vec::new(),
            conversation: ConversationState::new(),
            approvals: BTreeMap::new(),
            tool_executions: BTreeMap::new(),
            artifact_persistences: BTreeMap::new(),
            automatic_memory_writes: BTreeMap::new(),
            context_summaries: BTreeMap::new(),
            child_agents: BTreeMap::new(),
            received_child_messages: Vec::new(),
            planner_worker: PlannerWorkerState::default(),
            plugins: PluginExecutionState::default(),
            plugin_context_transforms: BTreeMap::new(),
            plugin_context_operations: BTreeMap::new(),
            mcp_oauth_audits: Vec::new(),
            process_reconciliations: BTreeMap::new(),
            last_sequence: Sequence::new(3).expect("sequence"),
            last_event_checksum: ContentHash::digest(b"head"),
        }
    }

    fn prepared(
        graph: &ExecutableGraph,
        reducer: &CanonicalVariableEventReducer,
        operations: &[CoordinatedVariableOperation],
    ) -> Vec<PreparedVariableEvent> {
        CanonicalVariableCoordinator::new(reducer, graph, &work())
            .expect("coordinator")
            .prepare_batch(operations)
            .expect("prepared")
    }

    fn command(
        graph: &ExecutableGraph,
        events: Vec<PreparedVariableEvent>,
        artifacts: Vec<ArtifactReference>,
    ) -> CommitVariableBatchCommand {
        CommitVariableBatchCommand {
            session_id: session_id(),
            graph: graph.clone(),
            events,
            artifacts,
        }
    }

    fn batch_operations(artifact_id: &str) -> Vec<CoordinatedVariableOperation> {
        vec![
            CoordinatedVariableOperation::Assign {
                variable: String::from("one"),
                expected_version: None,
                value: CanonicalVariableValue::String(String::from("first")),
                branch: None,
            },
            CoordinatedVariableOperation::Assign {
                variable: String::from("artifact"),
                expected_version: None,
                value: CanonicalVariableValue::ArtifactReference(artifact_id.to_owned()),
                branch: None,
            },
            CoordinatedVariableOperation::Assign {
                variable: String::from("two"),
                expected_version: None,
                value: CanonicalVariableValue::Integer(2),
                branch: None,
            },
        ]
    }

    fn artifact() -> ArtifactReference {
        let hash = ContentHash::digest(b"artifact");
        ArtifactReference {
            id: ArtifactIdentifier::parse(format!("blake3:{hash}")).expect("artifact id"),
            content_hash: hash,
        }
    }

    fn production_data() -> impl LocalRuntimeDataPort {
        local_runtime_data_with_artifacts()
    }

    fn production_envelope(
        sequence: u64,
        event_id: u128,
        payload: RuntimeCommittedEvent,
        artifacts: Vec<ArtifactReference>,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(event_id)),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(
                    i64::try_from(sequence).expect("timestamp sequence"),
                ),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(900)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(event_id.saturating_sub(1))),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts,
                classification: EventClassification::Committed,
            },
            payload,
        )
        .expect("production event")
    }

    fn seed_production_session<D>(
        data: &D,
        session_directory: &std::path::Path,
    ) -> EventEnvelope<RuntimeCommittedEvent>
    where
        D: Clone + JournalEventDataPort,
    {
        let created = production_envelope(
            1,
            100,
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: String::from("fixture"),
                style: String::from("persistent-chat"),
                style_binding: None,
            }),
            Vec::new(),
        );
        SessionPersistenceLogic::new(data.clone())
            .commit_event(CommitSessionEventCommand {
                session_directory: session_directory.to_owned(),
                event: created.clone(),
                durability: CommitDurability::Full,
            })
            .expect("seed session");
        created
    }

    #[test]
    fn every_batch_crash_prefix_recovers_without_duplicate_commits() {
        let graph = graph();
        let reducer = initial_reducer(&graph);
        let artifact = artifact();
        let artifact_reference = format!("artifact:{}", artifact.id.as_str());
        let events = prepared(&graph, &reducer, &batch_operations(&artifact_reference));
        for prefix in 0..=events.len() {
            let journal = MockJournal::new(session_state(&graph, reducer.clone()));
            journal.register_artifact(artifact.clone());
            if prefix == 0 {
                journal.cut_before_next();
            } else {
                journal.cut_after(prefix);
            }
            let coordinator = VariableTurnCoordinator::new(journal.clone());
            let command = command(&graph, events.clone(), vec![artifact.clone()]);
            assert!(coordinator.commit_batch(&command).is_err());

            let recovered = coordinator.recover_batch(&command).expect("recover");
            assert_eq!(recovered.committed + recovered.already_applied, 3);
            assert_eq!(journal.event_count(), 3);
            let state = journal.snapshot();
            let values = state
                .style_execution
                .expect("execution")
                .canonical_variables
                .expect("variables");
            assert_eq!(values.environment().values()["two"].version, 1);
        }
    }

    #[test]
    fn duplicate_receipt_and_cas_conflict_are_reclassified_from_replay() {
        let graph = graph();
        let reducer = initial_reducer(&graph);
        let events = prepared(
            &graph,
            &reducer,
            &[CoordinatedVariableOperation::Assign {
                variable: String::from("one"),
                expected_version: None,
                value: CanonicalVariableValue::String(String::from("first")),
                branch: None,
            }],
        );
        let journal = MockJournal::new(session_state(&graph, reducer));
        journal.conflict_once();
        let coordinator = VariableTurnCoordinator::new(journal.clone());
        let command = command(&graph, events, Vec::new());
        let first = coordinator.commit_batch(&command).expect("commit");
        assert_eq!(first.committed, 1);
        let sequence = first.last_sequence;
        let duplicate = coordinator.recover_batch(&command).expect("duplicate");
        assert_eq!(duplicate.committed, 0);
        assert_eq!(duplicate.already_applied, 1);
        assert_eq!(duplicate.last_sequence, sequence);
        assert_eq!(journal.event_count(), 1);
    }

    #[test]
    fn conflicting_retry_and_inactive_work_fail_before_append() {
        let graph = graph();
        let reducer = initial_reducer(&graph);
        let original = prepared(
            &graph,
            &reducer,
            &[CoordinatedVariableOperation::Assign {
                variable: String::from("one"),
                expected_version: None,
                value: CanonicalVariableValue::String(String::from("first")),
                branch: None,
            }],
        );
        let conflicting = prepared(
            &graph,
            &reducer,
            &[CoordinatedVariableOperation::Assign {
                variable: String::from("one"),
                expected_version: None,
                value: CanonicalVariableValue::String(String::from("different")),
                branch: None,
            }],
        );
        let journal = MockJournal::new(session_state(&graph, reducer));
        let coordinator = VariableTurnCoordinator::new(journal.clone());
        coordinator
            .commit_batch(&command(&graph, original, Vec::new()))
            .expect("first");
        assert!(
            coordinator
                .recover_batch(&command(&graph, conflicting, Vec::new()))
                .is_err()
        );
        assert_eq!(journal.event_count(), 1);

        let mut inactive = journal.snapshot();
        inactive
            .style_execution
            .as_mut()
            .expect("execution")
            .control = StyleExecutionControlState::Terminal {
            reason: String::from("done"),
        };
        let inactive_journal = MockJournal::new(inactive);
        let inactive_coordinator = VariableTurnCoordinator::new(inactive_journal.clone());
        let receipt = prepared(
            &graph,
            &initial_reducer(&graph),
            &[CoordinatedVariableOperation::Assign {
                variable: String::from("two"),
                expected_version: None,
                value: CanonicalVariableValue::Integer(2),
                branch: None,
            }],
        );
        assert!(
            inactive_coordinator
                .commit_batch(&command(&graph, receipt, Vec::new()))
                .is_err()
        );
        assert_eq!(inactive_journal.event_count(), 0);
    }

    #[test]
    fn validation_failure_and_branch_merge_commit_as_canonical_payloads() {
        let graph = graph();
        let reducer = initial_reducer(&graph);
        let operations = vec![
            CoordinatedVariableOperation::Assign {
                variable: String::from("two"),
                expected_version: None,
                value: CanonicalVariableValue::String(String::from("wrong type")),
                branch: None,
            },
            CoordinatedVariableOperation::Merge {
                variable: String::from("shared"),
                expected_version: None,
                branches: vec![
                    BranchVariableValue {
                        branch_id: String::from("b"),
                        stable_order: 1,
                        value: CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(
                            2,
                        )]),
                    },
                    BranchVariableValue {
                        branch_id: String::from("a"),
                        stable_order: 0,
                        value: CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(
                            1,
                        )]),
                    },
                ],
            },
        ];
        let receipts = prepared(&graph, &reducer, &operations);
        let journal = MockJournal::new(session_state(&graph, reducer));
        let coordinator = VariableTurnCoordinator::new(journal.clone());
        coordinator
            .commit_batch(&command(&graph, receipts, Vec::new()))
            .expect("commit");
        let state = journal.snapshot();
        let variables = state
            .style_execution
            .expect("execution")
            .canonical_variables
            .expect("variables");
        assert_eq!(variables.validation_failures().len(), 1);
        assert_eq!(
            variables.environment().values()["shared"].value,
            CanonicalVariableValue::List(vec![
                CanonicalVariableValue::Integer(1),
                CanonicalVariableValue::Integer(2),
            ])
        );
    }

    #[test]
    fn artifact_resolution_and_restart_transition_environment_are_exact() {
        let graph = graph();
        let reducer = initial_reducer(&graph);
        let artifact = artifact();
        let receipts = prepared(
            &graph,
            &reducer,
            &[CoordinatedVariableOperation::Assign {
                variable: String::from("artifact"),
                expected_version: None,
                value: CanonicalVariableValue::ArtifactReference(format!(
                    "artifact:{}",
                    artifact.id.as_str()
                )),
                branch: None,
            }],
        );
        let journal = MockJournal::new(session_state(&graph, reducer));
        journal.register_artifact(artifact.clone());
        let coordinator = VariableTurnCoordinator::new(journal.clone());
        coordinator
            .commit_batch(&command(&graph, receipts, vec![artifact.clone()]))
            .expect("commit");
        assert_eq!(journal.last_event().metadata.artifacts, vec![artifact]);

        let required = BTreeSet::from([String::from("input")]);
        let before = coordinator
            .transition_environment(session_id(), &graph, &work(), &required)
            .expect("environment");
        let restarted = MockJournal::new(journal.snapshot());
        let after = VariableTurnCoordinator::new(restarted)
            .transition_environment(session_id(), &graph, &work(), &required)
            .expect("environment");
        assert_eq!(before, after);
        assert_eq!(after["input"], true);
    }

    #[test]
    fn production_adapter_atomically_appends_replays_and_suppresses_restart_duplicate() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let session_directory = temporary.path().join("session");
        let artifact_store = temporary.path().join("artifacts");
        let data = production_data();
        let created = seed_production_session(&data, &session_directory);
        let adapter = SessionVariableTurnJournal::new(
            data.clone(),
            session_id(),
            session_directory.clone(),
            artifact_store.clone(),
        );
        let initial = adapter.load().expect("initial replay");
        assert_eq!(initial.state.last_sequence, Sequence::FIRST);
        assert_eq!(initial.last_event_id, created.metadata.event_id);

        let suspended = production_envelope(
            2,
            101,
            RuntimeCommittedEvent::SessionLifecycleChanged(SessionLifecycleChangedEvent {
                lifecycle: SessionLifecycle::Suspended,
                reason: Some(String::from("adapter fixture")),
            }),
            Vec::new(),
        );
        assert_eq!(
            adapter
                .append(
                    VariableTurnAppendPosition {
                        sequence: Sequence::FIRST,
                        event_id: created.metadata.event_id,
                    },
                    suspended.clone(),
                )
                .expect("append"),
            VariableTurnAppendOutcome::Appended
        );
        let replayed = adapter.load().expect("replay appended event");
        assert_eq!(replayed.state.lifecycle, SessionLifecycle::Suspended);
        assert_eq!(replayed.last_event_id, suspended.metadata.event_id);

        let restarted = SessionVariableTurnJournal::new(
            data.clone(),
            session_id(),
            session_directory.clone(),
            artifact_store,
        );
        assert_eq!(
            restarted
                .append(
                    VariableTurnAppendPosition {
                        sequence: Sequence::FIRST,
                        event_id: created.metadata.event_id,
                    },
                    suspended,
                )
                .expect("duplicate is a conflict"),
            VariableTurnAppendOutcome::Conflict
        );

        let active = production_envelope(
            3,
            102,
            RuntimeCommittedEvent::SessionLifecycleChanged(SessionLifecycleChangedEvent {
                lifecycle: SessionLifecycle::Active,
                reason: None,
            }),
            Vec::new(),
        );
        assert_eq!(
            restarted
                .append(
                    VariableTurnAppendPosition {
                        sequence: Sequence::new(2).expect("sequence"),
                        event_id: EventId::from_uuid(Uuid::from_u128(999)),
                    },
                    active,
                )
                .expect("stale identity is a conflict"),
            VariableTurnAppendOutcome::Conflict
        );
        assert_eq!(
            data.scan_events(ScanEventsDataRequest { session_directory })
                .expect("scan")
                .events
                .len(),
            2
        );
    }

    #[test]
    fn production_adapter_resolves_exact_artifacts_and_rejects_substitution() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let session_directory = temporary.path().join("session");
        let artifact_store = temporary.path().join("artifacts");
        let data = production_data();
        seed_production_session(&data, &session_directory);
        let persisted = data
            .persist_artifact(PersistArtifactDataRequest {
                store_root: artifact_store.clone(),
                creation_event: Uuid::from_u128(100).to_string(),
                producer: String::from("runtime"),
                mime_type: String::from("application/octet-stream"),
                bytes: b"exact immutable bytes".to_vec(),
                security: ArtifactSecurityRecord::Private,
                retention: ArtifactRetentionRecord::Session,
            })
            .expect("persist artifact");
        let exact = ArtifactReference {
            id: ArtifactIdentifier::parse(persisted.artifact_id).expect("artifact identifier"),
            content_hash: persisted.content_hash.parse().expect("content hash"),
        };
        let adapter =
            SessionVariableTurnJournal::new(data, session_id(), session_directory, artifact_store);
        let declared = BTreeSet::from([format!("artifact:{}", exact.id.as_str())]);
        assert_eq!(
            adapter
                .resolve_artifacts(&declared, std::slice::from_ref(&exact))
                .expect("resolve"),
            vec![exact.clone()]
        );

        let substituted = ArtifactReference {
            id: exact.id,
            content_hash: ContentHash::digest(b"substituted bytes"),
        };
        assert_eq!(
            adapter
                .resolve_artifacts(&declared, &[substituted])
                .expect_err("substitution rejected")
                .code,
            "artifact_substitution"
        );
    }
}
