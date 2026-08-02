//! Live outer coordination for generic parallel nodes and their bound joins.
//!
//! The coordinator owns journal ordering but not storage. It seals and reduces
//! every proposed event against the exact current head before asking the
//! injected journal boundary to append it. External branch effects remain
//! explicit and fail closed unless this module has a receipt-aware adapter.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use agentmod_event_model::{
    ArtifactIdentifier, ArtifactReference, EventClassification, EventEnvelope, EventMetadata,
    EventOrigin, EventScope,
};
use agentmod_graph_engine::{ExecutableGraph, VariableScope};
use agentmod_primitives::{
    CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId, TimestampMillis, Version,
};
use agentmod_runtime_data::{
    artifact::{ArtifactDataPort, InspectArtifactDataRequest},
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

use crate::{
    canonical_variables::{
        BranchVariableValue, BranchWriteContext, CanonicalVariableValue, VariableWriter,
        artifact_references, canonical_value_from_json, merge_branch_contributions,
    },
    node_execution::{
        CanonicalBudgetState, CanonicalGraphState, ExecuteNodeCommand, NodeExecutionInput,
        NodeExecutionOutcome, NodeExecutionOutput, NodeWorkIdentity, UserSpaceEventProposal,
        canonical_user_event_artifacts, execute_native_node,
    },
    parallel_driver::{
        BranchEffectDispatchClass, BranchEffectKind, BranchEffectRequest, DriveJoinCommand,
        DriveParallelCommand, InitializeJoinDriverCommand, InitializeParallelDriverCommand,
        NativePureBranchExecutor, ParallelDriverError, PureBranchExecutor, drive_join,
        drive_parallel, initialize_join, initialize_parallel,
    },
    parallel_execution::{JoinDecision, JoinMemberResult},
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort,
    },
    session::{
        CanonicalParallelExecutionState, GenericJoinLifecycleState, ParallelBranchControlState,
        ParallelBranchEffectDispatchedEvent, ParallelBranchEffectIdentity,
        ParallelBranchEffectOutcome as CanonicalBranchEffectOutcome,
        ParallelBranchEffectOutcomeRecordedEvent, ParallelBranchEffectOutput,
        ParallelBranchNodeCompletedEvent, ParallelBranchNodeFailedEvent,
        ParallelVariableContributionRecordedEvent, RuntimeCommittedEvent,
        SessionNodeExecutorResolution, SessionReducerError, SessionState, StyleExecutionContract,
        StyleExecutionControlState, StyleNodeCompletedEvent, StyleNodeEnteredEvent,
        StyleTransitionSelectedEvent, UserSpaceEventEmittedEvent,
        parallel_branch_effect_receipt_hash, reduce,
    },
};

const MAX_COORDINATOR_ROUNDS: usize = 4_096;

enum OuterControl {
    ParallelActive(Option<Box<CanonicalParallelExecutionState>>),
    JoinActive {
        parallel: Box<CanonicalParallelExecutionState>,
        join_work: NodeWorkIdentity,
        join_executor: Box<SessionNodeExecutorResolution>,
    },
    Advanced {
        node_id: String,
        step: u64,
    },
    RepairCommitted,
}

/// Exact journal head consumed by one coordinator advancement.
#[derive(Clone, Debug)]
pub struct ParallelTurnHead {
    /// Pure replayed canonical session state.
    pub state: SessionState,
    /// Event identity at the exact journal head.
    pub last_event_id: EventId,
}

/// Logic-owned event identity allocated by the outer runtime boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParallelTurnEventIdentity {
    /// Unique canonical event identity.
    pub event_id: EventId,
    /// Runtime-resolved event time.
    pub timestamp: TimestampMillis,
    /// Session/run correlation identity.
    pub correlation_id: CorrelationId,
    /// Default causation identity, replaced by the current head when appending.
    pub causation_id: CausationId,
}

/// Expected append position supplied to the journal CAS boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParallelTurnAppendPosition {
    /// Current canonical sequence.
    pub sequence: Sequence,
    /// Current canonical event identity.
    pub event_id: EventId,
}

/// Stable journal-boundary failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("parallel turn journal failed: {code}")]
pub struct ParallelTurnJournalError {
    /// Bounded diagnostic code.
    pub code: String,
}

/// Narrow outer boundary used by the live parallel coordinator.
pub trait ParallelTurnJournal: Send + Sync + 'static {
    /// Loads and purely replays the exact current session head.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when the journal cannot be loaded or replayed.
    fn load(&self) -> Result<ParallelTurnHead, ParallelTurnJournalError>;

    /// Allocates a runtime-owned event identity and exact timestamp.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when identity allocation fails.
    fn allocate_identity(&self) -> Result<ParallelTurnEventIdentity, ParallelTurnJournalError>;

    /// Appends one already sealed and reducer-validated canonical event.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error on head conflict or durable append failure.
    fn append(
        &self,
        expected: ParallelTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<(), ParallelTurnJournalError>;

    /// Resolves declared immutable artifacts before a user-space event commit.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when a declared artifact is unavailable
    /// or differs from the expected immutable identity.
    fn resolve_artifacts(
        &self,
        declared: &BTreeSet<String>,
        expected: &[ArtifactReference],
    ) -> Result<Vec<ArtifactReference>, ParallelTurnJournalError>;
}

/// Production journal adapter for the live runtime turn path.
///
/// This adapter remains in runtime logic and depends only on the runtime data
/// ports plus the existing persistence logic boundary. It never opens a
/// dependency implementation or journal file directly.
#[derive(Clone, Debug)]
pub struct TurnParallelJournal<D> {
    data: D,
    persistence: SessionPersistenceLogic<D>,
    session_id: SessionId,
    session_directory: PathBuf,
}

impl<D> TurnParallelJournal<D>
where
    D: Clone,
{
    /// Binds one exact session journal and its style-artifact namespace.
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

impl<D> ParallelTurnJournal for TurnParallelJournal<D>
where
    D: Clone
        + Send
        + Sync
        + ArtifactDataPort
        + EventIdentityDataPort
        + JournalEventDataPort
        + 'static,
{
    fn load(&self) -> Result<ParallelTurnHead, ParallelTurnJournalError> {
        let loaded = self
            .persistence
            .load_session(LoadSessionCommand {
                session_directory: self.session_directory.clone(),
                expected_session_id: self.session_id,
            })
            .map_err(|_| parallel_journal_error("load_failed"))?;
        Ok(ParallelTurnHead {
            state: loaded.state,
            last_event_id: loaded.last_event_id,
        })
    }

    fn allocate_identity(&self) -> Result<ParallelTurnEventIdentity, ParallelTurnJournalError> {
        self.data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map(|identity| ParallelTurnEventIdentity {
                event_id: identity.event_id,
                timestamp: identity.timestamp,
                correlation_id: identity.correlation_id,
                causation_id: identity.causation_id,
            })
            .map_err(|_| parallel_journal_error("identity_unavailable"))
    }

    fn append(
        &self,
        expected: ParallelTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<(), ParallelTurnJournalError> {
        if event.metadata.sequence
            != expected
                .sequence
                .checked_next()
                .map_err(|_| parallel_journal_error("append_sequence_overflow"))?
        {
            return Err(parallel_journal_error("append_conflict"));
        }
        let event_id = event.metadata.event_id;
        let sequence = event.metadata.sequence;
        let result = self
            .persistence
            .compare_append_event(CompareAppendSessionEventCommand {
                session_directory: self.session_directory.clone(),
                expected_head_event_id: expected.event_id,
                event,
                durability: CommitDurability::Data,
            })
            .map_err(|_| parallel_journal_error("append_failed"))?;
        let CompareAppendSessionEventResult::Appended(committed) = result else {
            return Err(parallel_journal_error("append_conflict"));
        };
        if committed.event_id != event_id || committed.sequence != sequence {
            return Err(parallel_journal_error("append_receipt_mismatch"));
        }
        Ok(())
    }

    fn resolve_artifacts(
        &self,
        declared: &BTreeSet<String>,
        expected: &[ArtifactReference],
    ) -> Result<Vec<ArtifactReference>, ParallelTurnJournalError> {
        if declared.len() != expected.len() {
            return Err(parallel_journal_error("artifact_count_mismatch"));
        }
        declared
            .iter()
            .zip(expected)
            .map(|(reference, expected)| {
                let record = self
                    .data
                    .inspect_artifact(InspectArtifactDataRequest {
                        store_root: self.session_directory.join("artifacts").join("style"),
                        artifact_reference: reference.clone(),
                    })
                    .map_err(|_| parallel_journal_error("artifact_unavailable"))?;
                let identifier = ArtifactIdentifier::parse(record.artifact_id)
                    .map_err(|_| parallel_journal_error("artifact_identity_invalid"))?;
                let content_hash = ContentHash::from_str(&record.content_hash)
                    .map_err(|_| parallel_journal_error("artifact_hash_invalid"))?;
                if record.artifact_reference != *reference
                    || identifier != expected.id
                    || content_hash != expected.content_hash
                {
                    return Err(parallel_journal_error("artifact_identity_mismatch"));
                }
                Ok(expected.clone())
            })
            .collect()
    }
}

fn parallel_journal_error(code: &'static str) -> ParallelTurnJournalError {
    ParallelTurnJournalError {
        code: code.to_owned(),
    }
}

/// One exact live parallel-root execution.
#[derive(Clone, Debug)]
pub struct DriveParallelTurnCommand {
    /// Owning session.
    pub session_id: SessionId,
    /// Immutable compiled graph.
    pub graph: ExecutableGraph,
    /// Immutable persisted execution contract.
    pub contract: StyleExecutionContract,
    /// Exact active root parallel work.
    pub root_work: NodeWorkIdentity,
    /// Exact persisted executor for the root parallel node.
    pub root_executor: SessionNodeExecutorResolution,
    /// Canonical transition variables.
    pub variables: Value,
    /// Effective global graph-step ceiling.
    pub max_steps: u64,
    /// Optional canonical cancellation request.
    pub cancellation_code: Option<String>,
}

/// Terminal result of one bounded coordinator call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParallelTurnResult {
    /// Current canonical sequence after all proposed commits.
    pub last_sequence: Sequence,
    /// Stable live outcome.
    pub outcome: ParallelTurnOutcome,
}

/// Live coordinator outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParallelTurnOutcome {
    /// Parallel root and generic join completed; this exact root node is active.
    Advanced {
        /// Compiled destination after the join.
        node_id: String,
        /// Globally serialized destination step.
        step: u64,
    },
    /// Canonical cancellation won and no further branch effect was dispatched.
    Cancelled,
    /// The join is canonically terminally unsuccessful.
    JoinFailed {
        /// Stable serialized failure class.
        reason: String,
    },
    /// One or more branches are durably waiting without redispatch.
    Waiting {
        /// Stable runtime branch identity.
        branch_id: String,
        /// Exact persisted node-work identity.
        work: NodeWorkIdentity,
        /// Durable continuation/wait reference.
        continuation_reference: String,
    },
}

fn ready_join_result_reference(
    ready: &crate::parallel_execution::JoinReadyDescriptor,
) -> Result<String, ParallelTurnError> {
    Ok(format!(
        "join:{}",
        agentmod_primitives::ContentHash::digest(
            &serde_json::to_vec(ready).map_err(|_| ParallelTurnError::Serialization)?
        )
    ))
}

/// Exact replay-aware invocation supplied to a branch-effect adapter.
#[derive(Clone, Debug, PartialEq)]
pub struct ParallelBranchEffectCommand {
    /// Canonical session.
    pub session_id: SessionId,
    /// Full persisted request identity.
    pub request: BranchEffectRequest,
    /// Full canonical outbox identity.
    pub identity: ParallelBranchEffectIdentity,
    /// Immutable transition environment.
    pub variables: Value,
    /// Runtime-enforced branch budget.
    pub budget: CanonicalBudgetState,
    /// Latest canonical wait/terminal outcome when reconciling.
    pub prior_outcome: Option<CanonicalBranchEffectOutcome>,
}

impl ParallelBranchEffectCommand {
    /// Reconstructs the exact fail-closed branch write context for a
    /// plugin-host invocation. This is the only context the turn adapter may
    /// pass to `execute_live_plugin_node_in_branch`.
    ///
    /// # Errors
    ///
    /// Rejects runtime executors, substituted branch/work/outbox identities,
    /// or an input that no longer matches the canonical outbox hash.
    pub fn plugin_branch_context(
        &self,
    ) -> Result<BranchWriteContext, ParallelBranchEffectPortError> {
        if !matches!(
            self.request.dispatch_class(),
            Ok(BranchEffectDispatchClass::Plugin)
        ) || self.request.branch_id.is_empty()
            || self.request.work.branch_path.last() != Some(&self.request.branch_id)
            || self.identity.branch_id != self.request.branch_id
            || self.identity.dispatch_id != self.request.dispatch_id
            || self.identity.work != self.request.work
            || self.identity.executor != self.request.executor
            || self.identity.effect_kind != "plugin"
            || serde_json::to_vec(&self.variables)
                .map(|bytes| ContentHash::digest(&bytes))
                .map_or(true, |hash| hash != self.identity.input_hash)
        {
            return Err(parallel_effect_port_error(
                "parallel_plugin_identity_invalid",
            ));
        }
        Ok(BranchWriteContext {
            branch_id: self.request.branch_id.clone(),
            stable_order: self.request.stable_order,
            serialized_shared_write: false,
        })
    }
}

/// Canonical terminal branch effect supplied for typed variable application.
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyParallelBranchEffectOutputCommand {
    /// Canonical session.
    pub session_id: SessionId,
    /// Exact immutable graph.
    pub graph: ExecutableGraph,
    /// Exact effect request whose terminal outcome is already canonical.
    pub request: BranchEffectRequest,
    /// Stable branch write identity reconstructed from the persisted fan-out.
    pub branch: BranchWriteContext,
    /// Exact canonical terminal effect output.
    pub output: NodeExecutionOutput,
    /// Whether the output came from an external/runtime effect adapter. Pure
    /// executor JSON is the ordinary typed output source; effect output must
    /// use only runtime-owned receipt slots.
    pub effect_output: bool,
}

/// Exact successful contribution set supplied to the join-owned canonical
/// merge boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParallelJoinVariableMerge {
    /// Declared shared variable.
    pub variable: String,
    /// Exact version captured before fan-out.
    pub base_version: Option<u64>,
    /// Successful configured-member contributions in stable compiled order.
    pub branches: Vec<BranchVariableValue>,
}

/// Join-ready canonical merge application.
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyParallelJoinMergesCommand {
    /// Canonical session.
    pub session_id: SessionId,
    /// Exact immutable graph.
    pub graph: ExecutableGraph,
    /// Exact fan-out owner.
    pub parallel_owner: NodeWorkIdentity,
    /// Exact active join work.
    pub join_work: NodeWorkIdentity,
    /// Stable result reference derived from the complete canonical join decision.
    pub result_reference: String,
    /// Complete deterministic merge set.
    pub merges: Vec<ParallelJoinVariableMerge>,
}

/// Typed bounded result returned by an injected branch-effect adapter.
#[derive(Clone, Debug, PartialEq)]
pub enum ParallelBranchEffectOutcome {
    /// The effect is durably waiting and must not be redispatched.
    Waiting {
        /// Stable continuation/wait reference.
        continuation_reference: String,
        /// Digest of the exact durable wait receipt.
        receipt_hash: ContentHash,
    },
    /// A definite terminal receipt permits node completion.
    Completed {
        /// Bounded node output validated by runtime orchestration.
        output: NodeExecutionOutput,
        /// Exact bounded plural artifact set retained outside the singular
        /// generic node-output ABI.
        artifact_references: BTreeSet<String>,
        /// Digest of the exact terminal receipt.
        receipt_hash: ContentHash,
        /// Constrained event proposal used only by the native emit adapter.
        emitted_event: Option<Box<UserSpaceEventProposal>>,
    },
    /// A definite terminal failure permits structured branch failure.
    Failed {
        /// Stable redacted failure code.
        code: String,
        /// Digest of the exact terminal receipt.
        receipt_hash: ContentHash,
    },
    /// The effect may have happened and automatic retry is prohibited.
    Ambiguous {
        /// Stable redacted ambiguity code.
        code: String,
        /// Digest of the available diagnostic/partial receipt.
        receipt_hash: ContentHash,
    },
}

/// Bounded adapter failure before a typed receipt could be reconstructed.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("parallel branch effect adapter failed: {code}")]
pub struct ParallelBranchEffectPortError {
    /// Stable redacted failure code.
    pub code: String,
}

fn parallel_effect_port_error(code: &str) -> ParallelBranchEffectPortError {
    ParallelBranchEffectPortError {
        code: code.to_owned(),
    }
}

fn bind_parallel_variable_base_versions(
    payload: &mut RuntimeCommittedEvent,
    state: &SessionState,
) -> Result<(), ParallelTurnError> {
    let RuntimeCommittedEvent::ParallelExecutionInitialized(initialized) = payload else {
        return Err(ParallelTurnError::Projection);
    };
    let entries = state
        .style_execution
        .as_ref()
        .and_then(|execution| execution.canonical_variables.as_deref())
        .ok_or(ParallelTurnError::Projection)?
        .environment()
        .canonical_entries();
    for region in &mut initialized.regions {
        region.variable_base_versions = region
            .write_variables
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    entries.get(name).map_or(0, |entry| entry.version),
                )
            })
            .collect::<BTreeMap<_, _>>();
    }
    Ok(())
}

/// Logic-owned external-effect boundary keyed by the exact persisted request.
#[async_trait]
pub trait ParallelBranchEffectPort: Send + Sync {
    /// Dispatches once immediately after the coordinator commits outbox intent.
    async fn dispatch(
        &self,
        command: ParallelBranchEffectCommand,
    ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError>;

    /// Reconciles an existing outbox/wait without automatically redispatching.
    async fn recover(
        &self,
        command: ParallelBranchEffectCommand,
    ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError>;

    /// Cancels or reconciles one exact plugin invocation after its branch
    /// outbox is canonical. The turn adapter must reuse the plugin node's
    /// keyed cancellation boundary and return a terminal receipt-derived
    /// outcome; automatic redispatch is forbidden.
    async fn cancel_plugin(
        &self,
        _command: ParallelBranchEffectCommand,
        _reason_code: String,
    ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
        Err(parallel_effect_port_error(
            "parallel_plugin_cancellation_unavailable",
        ))
    }

    /// Applies declared typed variable writes after the terminal effect outcome
    /// is canonical and before branch completion becomes canonical.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure when the exact declared output cannot
    /// be projected, recovered, or durably applied.
    fn apply_completed_output(
        &self,
        command: ApplyParallelBranchEffectOutputCommand,
    ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError>;

    /// Owns an effect-output application while it runs on a fresh task.
    ///
    /// Ports with an asynchronous pre-application boundary may override this
    /// method. Dropping the coordinator aborts that pending work before the
    /// synchronous canonical application begins.
    async fn apply_completed_output_owned(
        &self,
        command: ApplyParallelBranchEffectOutputCommand,
    ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
        self.apply_completed_output(command)
    }

    /// Applies a pure node output through the same typed variable boundary
    /// before its canonical node-completion event.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure when the exact declared output cannot
    /// be projected, recovered, or durably applied.
    fn apply_pure_completed_output(
        &self,
        command: ApplyParallelBranchEffectOutputCommand,
    ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
        self.apply_completed_output(command)
    }

    /// Commits join-owned canonical variable merges after readiness is
    /// canonical and before join completion.
    ///
    /// # Errors
    ///
    /// Returns a stable fail-closed boundary failure for missing,
    /// substituted, or conflicting merge receipts.
    fn apply_ready_join_merges(
        &self,
        command: ApplyParallelJoinMergesCommand,
    ) -> Result<(), ParallelBranchEffectPortError> {
        if command.merges.is_empty() {
            Ok(())
        } else {
            Err(parallel_effect_port_error(
                "parallel_join_merge_application_unavailable",
            ))
        }
    }
}

/// Default adapter for the runtime's pure constrained emit implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeParallelBranchEffectPort;

#[async_trait]
impl ParallelBranchEffectPort for NativeParallelBranchEffectPort {
    async fn dispatch(
        &self,
        command: ParallelBranchEffectCommand,
    ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
        execute_native_emit_effect(&command)
    }

    async fn recover(
        &self,
        command: ParallelBranchEffectCommand,
    ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
        execute_native_emit_effect(&command)
    }

    fn apply_completed_output(
        &self,
        command: ApplyParallelBranchEffectOutputCommand,
    ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
        let node = command
            .graph
            .nodes
            .iter()
            .find(|node| node.id == command.request.work.node_id)
            .ok_or_else(|| parallel_effect_port_error("branch_output_node_missing"))?;
        if !node.write_variables.is_empty() {
            return Err(parallel_effect_port_error(
                "branch_output_application_unavailable",
            ));
        }
        Ok(command.output)
    }

    fn apply_ready_join_merges(
        &self,
        command: ApplyParallelJoinMergesCommand,
    ) -> Result<(), ParallelBranchEffectPortError> {
        if command.merges.is_empty() {
            Ok(())
        } else {
            Err(parallel_effect_port_error(
                "parallel_join_merge_application_unavailable",
            ))
        }
    }
}

/// Live outer coordinator over one journal and one pure-node executor.
pub struct ParallelTurnCoordinator<
    J,
    E = NativePureBranchExecutor,
    F = NativeParallelBranchEffectPort,
> {
    journal: Arc<J>,
    pure_executor: Arc<E>,
    effect_port: Arc<F>,
}

impl<J> ParallelTurnCoordinator<J, NativePureBranchExecutor, NativeParallelBranchEffectPort> {
    /// Creates the production coordinator for native pure branch nodes.
    #[must_use]
    pub fn new(journal: J) -> Self {
        Self {
            journal: Arc::new(journal),
            pure_executor: Arc::new(NativePureBranchExecutor),
            effect_port: Arc::new(NativeParallelBranchEffectPort),
        }
    }
}

impl<J, E> ParallelTurnCoordinator<J, E, NativeParallelBranchEffectPort> {
    /// Creates a coordinator with an injectable pure executor.
    #[must_use]
    pub fn with_executor(journal: J, pure_executor: Arc<E>) -> Self {
        Self {
            journal: Arc::new(journal),
            pure_executor,
            effect_port: Arc::new(NativeParallelBranchEffectPort),
        }
    }
}

impl<J, E, F> ParallelTurnCoordinator<J, E, F> {
    /// Creates a coordinator with independently injected pure and effect ports.
    #[must_use]
    pub fn with_ports(journal: J, pure_executor: Arc<E>, effect_port: Arc<F>) -> Self {
        Self {
            journal: Arc::new(journal),
            pure_executor,
            effect_port,
        }
    }
}

impl<J, E, F> ParallelTurnCoordinator<J, E, F>
where
    J: ParallelTurnJournal,
    E: PureBranchExecutor,
    F: ParallelBranchEffectPort + 'static,
{
    /// Drives parallel members, their exact generic join, and the destination
    /// entry through one reducer-validated outer journal coordinator.
    ///
    /// # Errors
    ///
    /// Fails closed on canonical drift, budgets, unsupported branch effects,
    /// invalid receipts, append conflicts, or non-progressing projections.
    pub async fn drive(
        &self,
        command: DriveParallelTurnCommand,
    ) -> Result<ParallelTurnResult, ParallelTurnError> {
        let coordinator = Self {
            journal: Arc::clone(&self.journal),
            pure_executor: Arc::clone(&self.pure_executor),
            effect_port: Arc::clone(&self.effect_port),
        };
        crate::scoped_task::scoped_task(async move {
            Box::pin(coordinator.drive_on_fresh_task(command)).await
        })
        .await
        .map_err(|_| ParallelTurnError::CoordinatorTask)?
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the outer recovery loop keeps every journal cut and its exact next action adjacent"
    )]
    async fn drive_on_fresh_task(
        &self,
        command: DriveParallelTurnCommand,
    ) -> Result<ParallelTurnResult, ParallelTurnError> {
        self.validate_contract(&command)?;
        for _ in 0..MAX_COORDINATOR_ROUNDS {
            let parallel = match self.recover_outer_control(&command)? {
                OuterControl::ParallelActive(parallel) => parallel,
                OuterControl::JoinActive {
                    parallel,
                    join_work,
                    join_executor,
                } => {
                    if let Some(outcome) =
                        self.drive_bound_join(&command, &parallel, &join_work, *join_executor)?
                    {
                        return Ok(ParallelTurnResult {
                            last_sequence: self.journal.load()?.state.last_sequence,
                            outcome,
                        });
                    }
                    continue;
                }
                OuterControl::Advanced { node_id, step } => {
                    return Ok(ParallelTurnResult {
                        last_sequence: self.journal.load()?.state.last_sequence,
                        outcome: ParallelTurnOutcome::Advanced { node_id, step },
                    });
                }
                OuterControl::RepairCommitted => continue,
            };
            let head = self.journal.load()?;
            let Some(parallel) = parallel else {
                let mut payload = initialize_parallel(&InitializeParallelDriverCommand {
                    graph: command.graph.clone(),
                    owner: command.root_work.clone(),
                    executor: command.root_executor.clone(),
                })?;
                bind_parallel_variable_base_versions(&mut payload, &head.state)?;
                self.commit_payload(head, payload, Vec::new(), None)?;
                continue;
            };
            if command.cancellation_code.is_some() && parallel.cancellation_completed_at.is_some() {
                return Ok(ParallelTurnResult {
                    last_sequence: head.state.last_sequence,
                    outcome: ParallelTurnOutcome::Cancelled,
                });
            }
            if command.cancellation_code.is_some() && parallel.cancellation_requested_at.is_none() {
                let cancellation = drive_parallel(
                    DriveParallelCommand {
                        session_id: command.session_id,
                        graph: command.graph.clone(),
                        contract: command.contract.clone(),
                        parallel: *parallel.clone(),
                        variables: command.variables.clone(),
                        branch_variables: Self::branch_variable_inputs(
                            &head.state,
                            &command.graph,
                            &parallel,
                            &command.variables,
                        )?,
                        max_steps: command.max_steps,
                        cancellation_code: command.cancellation_code.clone(),
                    },
                    Arc::clone(&self.pure_executor),
                )
                .await?;
                let request = cancellation
                    .events
                    .into_iter()
                    .next()
                    .filter(|event| {
                        matches!(
                            event,
                            RuntimeCommittedEvent::ParallelCancellationRequested(_)
                        )
                    })
                    .ok_or(ParallelTurnError::NoProgress)?;
                self.commit_payload(head, request, Vec::new(), None)?;
                continue;
            }
            if let Some(code) = command.cancellation_code.as_deref()
                && self
                    .cancel_active_plugin_effect(&command, &parallel, code)
                    .await?
            {
                continue;
            }
            let output = Box::pin(drive_parallel(
                DriveParallelCommand {
                    session_id: command.session_id,
                    graph: command.graph.clone(),
                    contract: command.contract.clone(),
                    parallel: *parallel.clone(),
                    variables: command.variables.clone(),
                    branch_variables: Self::branch_variable_inputs(
                        &head.state,
                        &command.graph,
                        &parallel,
                        &command.variables,
                    )?,
                    max_steps: command.max_steps,
                    cancellation_code: command.cancellation_code.clone(),
                },
                Arc::clone(&self.pure_executor),
            ))
            .await?;
            if !output.events.is_empty() {
                let mut payload = output
                    .events
                    .into_iter()
                    .next()
                    .ok_or(ParallelTurnError::NoProgress)?;
                if let RuntimeCommittedEvent::ParallelBranchNodeCompleted(completed) = &mut payload
                {
                    completed.result = self.apply_pure_completed_output(&command, completed)?;
                }
                self.commit_payloads(vec![payload])?;
                continue;
            }
            if !output.effect_requests.is_empty() {
                let effects =
                    Box::pin(self.process_effect_requests(&command, &output.effect_requests))
                        .await?;
                if effects.progressed {
                    continue;
                }
                if let Some(waiting) = effects.waiting.into_iter().next() {
                    return Ok(ParallelTurnResult {
                        last_sequence: self.journal.load()?.state.last_sequence,
                        outcome: ParallelTurnOutcome::Waiting {
                            branch_id: waiting.branch_id,
                            work: waiting.work,
                            continuation_reference: waiting.continuation_reference,
                        },
                    });
                }
                return Err(ParallelTurnError::NoProgress);
            }
            if !parallel
                .branches
                .values()
                .all(|branch| matches!(branch.control, ParallelBranchControlState::Terminal { .. }))
            {
                return Err(ParallelTurnError::NoProgress);
            }
            self.complete_parallel_root(&command, &parallel)?;
        }
        Err(ParallelTurnError::RoundLimit)
    }

    fn validate_contract(
        &self,
        command: &DriveParallelTurnCommand,
    ) -> Result<(), ParallelTurnError> {
        let head = self.journal.load()?;
        let execution = head
            .state
            .style_execution
            .as_ref()
            .ok_or(ParallelTurnError::Projection)?;
        let planned = command
            .contract
            .node_executors
            .iter()
            .find(|resolution| resolution.node_id == command.root_work.node_id)
            .ok_or(ParallelTurnError::Projection)?;
        if execution.graph.as_ref() != &command.graph
            || execution.execution_contract.as_deref() != Some(&command.contract)
            || !command.root_work.branch_path.is_empty()
            || planned != &command.root_executor
            || command.root_work.run_id != command.contract.run_id
        {
            return Err(ParallelTurnError::Projection);
        }
        Ok(())
    }

    fn branch_variable_inputs(
        state: &SessionState,
        graph: &ExecutableGraph,
        parallel: &CanonicalParallelExecutionState,
        legacy_fallback: &Value,
    ) -> Result<BTreeMap<String, Value>, ParallelTurnError> {
        let mut inputs = BTreeMap::new();
        for (branch_id, branch) in &parallel.branches {
            let ParallelBranchControlState::Active(entered) = &branch.control else {
                continue;
            };
            inputs.insert(
                branch_id.clone(),
                Self::branch_input(state, graph, &entered.work, legacy_fallback)?,
            );
        }
        Ok(inputs)
    }

    fn branch_input(
        state: &SessionState,
        graph: &ExecutableGraph,
        work: &NodeWorkIdentity,
        legacy_fallback: &Value,
    ) -> Result<Value, ParallelTurnError> {
        if graph.variables.is_empty() {
            return Ok(legacy_fallback.clone());
        }
        crate::session::canonical_parallel_branch_node_input(state, work)
            .map_err(|_| ParallelTurnError::Projection)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive replay control states remain adjacent so every crash-gap repair and its legal next event can be audited together"
    )]
    fn recover_outer_control(
        &self,
        command: &DriveParallelTurnCommand,
    ) -> Result<OuterControl, ParallelTurnError> {
        let head = self.journal.load()?;
        let (control, transitions) = {
            let execution = head
                .state
                .style_execution
                .as_ref()
                .ok_or(ParallelTurnError::Projection)?;
            (execution.control.clone(), execution.transitions.clone())
        };
        let join_node_id = parallel_join_target(&command.graph, &command.root_work.node_id)?;
        match &control {
            StyleExecutionControlState::ReadyForEntry(cursor)
                if cursor.node_id == command.root_work.node_id
                    && cursor.attempt == command.root_work.attempt
                    && cursor.loop_iteration == command.root_work.loop_iteration
                    && cursor.step == command.root_work.step =>
            {
                self.commit_payload(
                    head,
                    RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                        node_id: cursor.node_id.clone(),
                        attempt: cursor.attempt,
                        loop_iteration: cursor.loop_iteration,
                        step: cursor.step,
                    }),
                    Vec::new(),
                    None,
                )?;
                Ok(OuterControl::RepairCommitted)
            }
            StyleExecutionControlState::Active(active)
                if active.node_id == command.root_work.node_id
                    && active.attempt == command.root_work.attempt
                    && active.loop_iteration == command.root_work.loop_iteration
                    && active.step == command.root_work.step =>
            {
                Ok(OuterControl::ParallelActive(
                    find_parallel(&head.state, &command.root_work)
                        .cloned()
                        .map(Box::new),
                ))
            }
            StyleExecutionControlState::AwaitingTransition(completed)
                if completed.node_id == command.root_work.node_id
                    && completed.attempt == command.root_work.attempt
                    && completed.loop_iteration == command.root_work.loop_iteration
                    && completed.step == command.root_work.step =>
            {
                let parallel = find_parallel(&head.state, &command.root_work)
                    .ok_or(ParallelTurnError::Projection)?;
                if !parallel.branches.values().all(|branch| {
                    matches!(branch.control, ParallelBranchControlState::Terminal { .. })
                }) {
                    return Err(ParallelTurnError::Projection);
                }
                self.commit_payload(
                    head,
                    RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                        from_node_id: command.root_work.node_id.clone(),
                        to_node_id: join_node_id,
                        attempt: command.root_work.attempt,
                        loop_iteration: command.root_work.loop_iteration,
                        step: command.root_work.step,
                    }),
                    Vec::new(),
                    None,
                )?;
                Ok(OuterControl::RepairCommitted)
            }
            StyleExecutionControlState::AwaitingDestinationEntry(selected)
                if selected.from_node_id == command.root_work.node_id
                    && selected.to_node_id == join_node_id
                    && selected.attempt == command.root_work.attempt
                    && selected.loop_iteration == command.root_work.loop_iteration
                    && selected.step == command.root_work.step =>
            {
                let parallel = find_parallel(&head.state, &command.root_work)
                    .ok_or(ParallelTurnError::Projection)?;
                let join_step = parallel
                    .last_allocated_step
                    .checked_add(1)
                    .ok_or(ParallelTurnError::Budget)?;
                if join_step > command.max_steps {
                    return Err(ParallelTurnError::Budget);
                }
                self.commit_payload(
                    head,
                    RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                        node_id: join_node_id,
                        attempt: selected.attempt,
                        loop_iteration: selected.loop_iteration,
                        step: join_step,
                    }),
                    Vec::new(),
                    None,
                )?;
                Ok(OuterControl::RepairCommitted)
            }
            StyleExecutionControlState::Active(active) if active.node_id == join_node_id => {
                let parallel = find_parallel(&head.state, &command.root_work)
                    .ok_or(ParallelTurnError::Projection)?
                    .clone();
                let expected_step = parallel
                    .last_allocated_step
                    .checked_add(1)
                    .ok_or(ParallelTurnError::Budget)?;
                if active.attempt != command.root_work.attempt
                    || active.loop_iteration != command.root_work.loop_iteration
                    || active.step != expected_step
                {
                    return Err(ParallelTurnError::Projection);
                }
                let join_executor = command
                    .contract
                    .node_executors
                    .iter()
                    .find(|resolution| resolution.node_id == join_node_id)
                    .ok_or(ParallelTurnError::Projection)?
                    .clone();
                Ok(OuterControl::JoinActive {
                    parallel: Box::new(parallel),
                    join_work: NodeWorkIdentity {
                        run_id: command.contract.run_id.clone(),
                        node_id: join_node_id,
                        branch_path: Vec::new(),
                        attempt: active.attempt,
                        loop_iteration: active.loop_iteration,
                        step: active.step,
                    },
                    join_executor: Box::new(join_executor),
                })
            }
            StyleExecutionControlState::AwaitingTransition(completed)
                if completed.node_id == join_node_id =>
            {
                let destination = join_destination(&command.graph, &join_node_id)?;
                let join = find_join(
                    &head.state,
                    &NodeWorkIdentity {
                        run_id: command.contract.run_id.clone(),
                        node_id: join_node_id.clone(),
                        branch_path: Vec::new(),
                        attempt: completed.attempt,
                        loop_iteration: completed.loop_iteration,
                        step: completed.step,
                    },
                )
                .ok_or(ParallelTurnError::Projection)?;
                if !matches!(join.lifecycle, GenericJoinLifecycleState::Ready(_)) {
                    return Err(ParallelTurnError::Projection);
                }
                self.commit_payload(
                    head,
                    RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                        from_node_id: join_node_id,
                        to_node_id: destination,
                        attempt: completed.attempt,
                        loop_iteration: completed.loop_iteration,
                        step: completed.step,
                    }),
                    Vec::new(),
                    None,
                )?;
                Ok(OuterControl::RepairCommitted)
            }
            StyleExecutionControlState::AwaitingDestinationEntry(selected)
                if selected.from_node_id == join_node_id =>
            {
                let destination = join_destination(&command.graph, &join_node_id)?;
                if selected.to_node_id != destination {
                    return Err(ParallelTurnError::Projection);
                }
                let step = selected
                    .step
                    .checked_add(1)
                    .ok_or(ParallelTurnError::Budget)?;
                if step > command.max_steps {
                    return Err(ParallelTurnError::Budget);
                }
                self.commit_payload(
                    head,
                    RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                        node_id: destination,
                        attempt: selected.attempt,
                        loop_iteration: selected.loop_iteration,
                        step,
                    }),
                    Vec::new(),
                    None,
                )?;
                Ok(OuterControl::RepairCommitted)
            }
            StyleExecutionControlState::Active(active)
                if transitions.last().is_some_and(|selected| {
                    selected.from_node_id == join_node_id
                        && selected.to_node_id == active.node_id
                        && selected.attempt == active.attempt
                        && selected.loop_iteration == active.loop_iteration
                }) =>
            {
                Ok(OuterControl::Advanced {
                    node_id: active.node_id.clone(),
                    step: active.step,
                })
            }
            StyleExecutionControlState::ReadyForEntry(_)
            | StyleExecutionControlState::Active(_)
            | StyleExecutionControlState::AwaitingTransition(_)
            | StyleExecutionControlState::AwaitingDestinationEntry(_)
            | StyleExecutionControlState::Terminal { .. } => Err(ParallelTurnError::Projection),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the effect outbox state machine is kept in one place so dispatch, recovery, waiting, and terminal handling stay visibly exhaustive"
    )]
    async fn process_effect_requests(
        &self,
        command: &DriveParallelTurnCommand,
        requests: &[BranchEffectRequest],
    ) -> Result<EffectProcessing, ParallelTurnError> {
        let mut result = EffectProcessing::default();
        for request in requests {
            let head = self.journal.load()?;
            let active = active_branch_entry(&head.state, &request.work)
                .ok_or(ParallelTurnError::Projection)?
                .clone();
            let effect = branch_effect_record(&head.state, request).cloned();
            if let Some(effect) = effect {
                validate_persisted_branch_effect_identity(request, &active, &effect.identity)?;
                match effect.outcome {
                    Some(CanonicalBranchEffectOutcome::Completed { output, .. }) => {
                        if request.kind == BranchEffectKind::EmitEvent {
                            self.validate_existing_emit_receipt(command, request)?;
                        }
                        let artifact_references = output.artifact_references.clone();
                        let output = self
                            .apply_completed_output(
                                command,
                                request,
                                node_output_from_canonical(output),
                            )
                            .await?;
                        self.commit_branch_completion(request, output, artifact_references)?;
                        result.progressed = true;
                    }
                    Some(CanonicalBranchEffectOutcome::Failed { code, .. }) => {
                        self.commit_branch_failure(request, &code)?;
                        result.progressed = true;
                    }
                    Some(CanonicalBranchEffectOutcome::Ambiguous { code, .. }) => {
                        self.commit_branch_failure(request, &format!("ambiguous:{code}"))?;
                        result.progressed = true;
                    }
                    prior @ (None | Some(CanonicalBranchEffectOutcome::Waiting { .. })) => {
                        let variables = Self::branch_input(
                            &head.state,
                            &command.graph,
                            &request.work,
                            &command.variables,
                        )?;
                        let reconstructed = branch_effect_identity(request, &active, &variables)?;
                        if reconstructed != effect.identity {
                            return Err(ParallelTurnError::EffectReceiptConflict);
                        }
                        let outcome = Box::pin(self.effect_port.recover(effect_command(
                            command,
                            request,
                            effect.identity.clone(),
                            variables.clone(),
                            prior.clone(),
                        )?))
                        .await
                        .map_err(ParallelTurnError::EffectPort)?;
                        if let ParallelBranchEffectOutcome::Waiting {
                            continuation_reference,
                            ..
                        } = &outcome
                        {
                            result.waiting.push(EffectWaiting {
                                branch_id: request.branch_id.clone(),
                                work: request.work.clone(),
                                continuation_reference: continuation_reference.clone(),
                            });
                        }
                        if prior.as_ref() == Some(&canonical_effect_outcome(&outcome)) {
                            continue;
                        }
                        self.commit_effect_outcome(request, &effect.identity, &outcome)?;
                        result.progressed = true;
                    }
                }
                continue;
            }
            if request.kind == BranchEffectKind::EmitEvent
                && head
                    .state
                    .style_execution
                    .as_ref()
                    .is_some_and(|execution| {
                        execution
                            .emitted_user_events
                            .iter()
                            .any(|record| record.event.work == request.work)
                    })
            {
                self.complete_emit_receipt(command, request)?;
                result.progressed = true;
                continue;
            }
            let variables = Self::branch_input(
                &head.state,
                &command.graph,
                &request.work,
                &command.variables,
            )?;
            let identity = branch_effect_identity(request, &active, &variables)?;
            self.commit_payload(
                head,
                RuntimeCommittedEvent::ParallelBranchEffectDispatched(
                    ParallelBranchEffectDispatchedEvent {
                        identity: identity.clone(),
                    },
                ),
                Vec::new(),
                None,
            )?;
            let outcome = Box::pin(self.effect_port.dispatch(effect_command(
                command,
                request,
                identity.clone(),
                variables,
                None,
            )?))
            .await
            .map_err(ParallelTurnError::EffectPort)?;
            if let ParallelBranchEffectOutcome::Waiting {
                continuation_reference,
                ..
            } = &outcome
            {
                result.waiting.push(EffectWaiting {
                    branch_id: request.branch_id.clone(),
                    work: request.work.clone(),
                    continuation_reference: continuation_reference.clone(),
                });
            }
            self.commit_effect_outcome(request, &identity, &outcome)?;
            result.progressed = true;
        }
        Ok(result)
    }

    async fn cancel_active_plugin_effect(
        &self,
        command: &DriveParallelTurnCommand,
        parallel: &CanonicalParallelExecutionState,
        reason_code: &str,
    ) -> Result<bool, ParallelTurnError> {
        for branch in parallel.branches.values() {
            let Some(effect) = branch.effect.as_ref() else {
                continue;
            };
            if matches!(
                effect.outcome,
                Some(
                    CanonicalBranchEffectOutcome::Completed { .. }
                        | CanonicalBranchEffectOutcome::Failed { .. }
                        | CanonicalBranchEffectOutcome::Ambiguous { .. }
                )
            ) {
                continue;
            }
            let active = active_branch_entry(&self.journal.load()?.state, &effect.identity.work)
                .ok_or(ParallelTurnError::Projection)?
                .clone();
            let node = command
                .graph
                .nodes
                .iter()
                .find(|node| node.id == active.work.node_id)
                .ok_or(ParallelTurnError::Projection)?;
            let request = BranchEffectRequest {
                branch_id: active.branch_id.clone(),
                dispatch_id: active.dispatch_id.clone(),
                stable_order: branch.region.member.branch_index,
                work: active.work.clone(),
                executor: active.executor.clone(),
                configuration: node.configuration.clone(),
                kind: BranchEffectKind::OtherRuntimeEffect,
            };
            if request.dispatch_class()? != BranchEffectDispatchClass::Plugin {
                continue;
            }
            validate_persisted_branch_effect_identity(&request, &active, &effect.identity)?;
            let head = self.journal.load()?;
            let variables = Self::branch_input(
                &head.state,
                &command.graph,
                &request.work,
                &command.variables,
            )?;
            let reconstructed = branch_effect_identity(&request, &active, &variables)?;
            if reconstructed != effect.identity {
                return Err(ParallelTurnError::EffectReceiptConflict);
            }
            let outcome = self
                .effect_port
                .cancel_plugin(
                    effect_command(
                        command,
                        &request,
                        effect.identity.clone(),
                        variables,
                        effect.outcome.clone(),
                    )?,
                    reason_code.to_owned(),
                )
                .await
                .map_err(ParallelTurnError::EffectPort)?;
            if matches!(outcome, ParallelBranchEffectOutcome::Waiting { .. }) {
                return Err(ParallelTurnError::InvalidEffectOutcome);
            }
            self.commit_effect_outcome(&request, &effect.identity, &outcome)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn commit_effect_outcome(
        &self,
        request: &BranchEffectRequest,
        identity: &ParallelBranchEffectIdentity,
        outcome: &ParallelBranchEffectOutcome,
    ) -> Result<(), ParallelTurnError> {
        if let ParallelBranchEffectOutcome::Completed {
            emitted_event: Some(event),
            ..
        } = &outcome
        {
            if request.kind != BranchEffectKind::EmitEvent {
                return Err(ParallelTurnError::InvalidEffectOutcome);
            }
            let artifacts = canonical_user_event_artifacts(&event.artifact_references)?;
            let resolved = self
                .journal
                .resolve_artifacts(&event.artifact_references, &artifacts)?;
            let head = self.journal.load()?;
            let existing = head.state.style_execution.as_ref().and_then(|execution| {
                execution
                    .emitted_user_events
                    .iter()
                    .find(|record| record.event.work == request.work)
            });
            if existing.is_none() {
                self.commit_payload(
                    head,
                    RuntimeCommittedEvent::UserSpaceEventEmitted(UserSpaceEventEmittedEvent {
                        work: request.work.clone(),
                        declared_event_type: event.declared_event_type.clone(),
                        payload: event.payload.clone(),
                        artifact_references: event.artifact_references.clone(),
                        metadata: event.metadata.clone(),
                    }),
                    resolved,
                    None,
                )?;
            } else {
                self.validate_existing_emit_proposal(request, event)?;
            }
        } else if request.kind == BranchEffectKind::EmitEvent
            && matches!(outcome, ParallelBranchEffectOutcome::Completed { .. })
        {
            return Err(ParallelTurnError::InvalidEffectOutcome);
        }
        let canonical = canonical_effect_outcome(outcome);
        let head = self.journal.load()?;
        self.commit_payload(
            head,
            RuntimeCommittedEvent::ParallelBranchEffectOutcomeRecorded(
                ParallelBranchEffectOutcomeRecordedEvent {
                    identity: identity.clone(),
                    outcome: canonical,
                },
            ),
            Vec::new(),
            None,
        )?;
        Ok(())
    }

    fn complete_emit_receipt(
        &self,
        command: &DriveParallelTurnCommand,
        request: &BranchEffectRequest,
    ) -> Result<(), ParallelTurnError> {
        if request.kind != BranchEffectKind::EmitEvent {
            return Err(ParallelTurnError::UnsupportedBranchEffect {
                node_id: request.work.node_id.clone(),
                kind: request.kind,
            });
        }
        let outcome = execute_native_node(&ExecuteNodeCommand {
            session_id: command.session_id,
            work: request.work.clone(),
            executor: request.executor.clone(),
            configuration: request.configuration.clone(),
            input: NodeExecutionInput {
                transition_variables: command.variables.clone(),
            },
            graph_state: CanonicalGraphState {
                attempt: request.work.attempt,
                loop_iteration: request.work.loop_iteration,
                step: request.work.step,
                completed_node_ids: Vec::new(),
            },
            budget_state: branch_budget(command, request)?,
        })?;
        let NodeExecutionOutcome::Emitted { event, output } = outcome else {
            return Err(ParallelTurnError::InvalidEffectOutcome);
        };
        let head = self.journal.load()?;
        let existing = head
            .state
            .style_execution
            .as_ref()
            .and_then(|execution| {
                execution
                    .emitted_user_events
                    .iter()
                    .find(|record| record.event.work == request.work)
            })
            .ok_or(ParallelTurnError::EffectReceiptConflict)?;
        let expected_artifacts = canonical_user_event_artifacts(&event.artifact_references)?;
        if existing.event.declared_event_type != event.declared_event_type
            || existing.event.payload != event.payload
            || existing.event.artifact_references != event.artifact_references
            || existing.event.metadata != event.metadata
            || existing.envelope_artifacts != expected_artifacts
        {
            return Err(ParallelTurnError::EffectReceiptConflict);
        }
        let variables = Self::branch_input(
            &head.state,
            &command.graph,
            &request.work,
            &command.variables,
        )?;
        let outcome = effect_outcome_with_hash(
            &branch_effect_identity(
                request,
                active_branch_entry(&head.state, &request.work)
                    .ok_or(ParallelTurnError::Projection)?,
                &variables,
            )?,
            ParallelBranchEffectOutcome::Completed {
                output,
                artifact_references: BTreeSet::new(),
                receipt_hash: ContentHash::from_bytes([0; 32]),
                emitted_event: Some(Box::new(event)),
            },
        );
        let identity = branch_effect_identity(
            request,
            active_branch_entry(&head.state, &request.work).ok_or(ParallelTurnError::Projection)?,
            &variables,
        )?;
        self.commit_effect_outcome(request, &identity, &outcome)
    }

    async fn apply_completed_output(
        &self,
        command: &DriveParallelTurnCommand,
        request: &BranchEffectRequest,
        output: NodeExecutionOutput,
    ) -> Result<NodeExecutionOutput, ParallelTurnError>
    where
        F: 'static,
    {
        let head = self.journal.load()?;
        let parallel =
            find_parallel(&head.state, &command.root_work).ok_or(ParallelTurnError::Projection)?;
        let active = active_branch_entry(&head.state, &request.work)
            .ok_or(ParallelTurnError::Projection)?
            .clone();
        let member = parallel
            .execution
            .member_bindings()
            .iter()
            .find(|member| member.branch_id == request.branch_id)
            .ok_or(ParallelTurnError::Projection)?;
        if request.stable_order != member.branch_index {
            return Err(ParallelTurnError::Projection);
        }
        let apply_command = ApplyParallelBranchEffectOutputCommand {
            session_id: command.session_id,
            graph: command.graph.clone(),
            request: request.clone(),
            branch: BranchWriteContext {
                branch_id: request.branch_id.clone(),
                stable_order: request.stable_order,
                // Shared writes remain fail-closed here. A merge policy
                // requires a separately persisted branch-contribution
                // receipt and a deterministic join-time Merge operation;
                // applying it as a direct branch assignment would erase
                // the other members' contributions.
                serialized_shared_write: false,
            },
            output,
            // Plugin proposals have already crossed the exact persisted
            // plugin-host identity and runtime output validator. Their typed
            // branch-local variables must therefore use the ordinary output
            // projection; runtime adapters remain limited to receipt slots.
            effect_output: request.dispatch_class()? != BranchEffectDispatchClass::Plugin,
        };
        let effect_port = Arc::clone(&self.effect_port);
        // Applying a terminal branch output can traverse the complete typed
        // variable replay/validation/commit boundary. Run that synchronous
        // boundary as a fresh task so approval-resume and generic graph
        // coordinator poll frames are fully unwound first; this keeps the
        // default Windows Tokio worker stack sufficient without changing
        // event ordering or effect authority.
        let applied = crate::scoped_task::scoped_task(async move {
            effect_port
                .apply_completed_output_owned(apply_command)
                .await
        })
        .await
        .map_err(|_| {
            ParallelTurnError::EffectPort(parallel_effect_port_error(
                "branch_output_application_task_failed",
            ))
        })?
        .map_err(ParallelTurnError::EffectPort)?;
        self.commit_shared_contributions(command, &active, &applied.transition_variables)?;
        Ok(applied)
    }

    fn apply_pure_completed_output(
        &self,
        command: &DriveParallelTurnCommand,
        completed: &ParallelBranchNodeCompletedEvent,
    ) -> Result<JoinMemberResult, ParallelTurnError> {
        let head = self.journal.load()?;
        let parallel =
            find_parallel(&head.state, &command.root_work).ok_or(ParallelTurnError::Projection)?;
        let member = parallel
            .execution
            .member_bindings()
            .iter()
            .find(|member| member.branch_id == completed.entered.branch_id)
            .ok_or(ParallelTurnError::Projection)?;
        let node = command
            .graph
            .nodes
            .iter()
            .find(|node| node.id == completed.entered.work.node_id)
            .ok_or(ParallelTurnError::Projection)?;
        let output = NodeExecutionOutput {
            result_reference: completed.result.node_result_reference.clone(),
            artifact_reference: completed.result.artifact_references.iter().next().cloned(),
            transition_variables: completed
                .result
                .inline_value
                .clone()
                .ok_or(ParallelTurnError::InvalidContribution)?,
        };
        let applied = self
            .effect_port
            .apply_pure_completed_output(ApplyParallelBranchEffectOutputCommand {
                session_id: command.session_id,
                graph: command.graph.clone(),
                request: BranchEffectRequest {
                    branch_id: completed.entered.branch_id.clone(),
                    dispatch_id: completed.entered.dispatch_id.clone(),
                    stable_order: member.branch_index,
                    work: completed.entered.work.clone(),
                    executor: completed.entered.executor.clone(),
                    configuration: node.configuration.clone(),
                    kind: BranchEffectKind::OtherRuntimeEffect,
                },
                branch: BranchWriteContext {
                    branch_id: completed.entered.branch_id.clone(),
                    stable_order: member.branch_index,
                    serialized_shared_write: false,
                },
                output,
                effect_output: false,
            })
            .map_err(ParallelTurnError::EffectPort)?;
        self.commit_shared_contributions(
            command,
            &completed.entered,
            &applied.transition_variables,
        )?;
        let artifact_references = applied
            .artifact_reference
            .into_iter()
            .collect::<BTreeSet<_>>();
        Ok(JoinMemberResult {
            inline_value: Some(applied.transition_variables),
            node_result_reference: applied.result_reference,
            declared_artifact_references: artifact_references.clone(),
            artifact_references,
        })
    }

    fn commit_shared_contributions(
        &self,
        command: &DriveParallelTurnCommand,
        entered: &crate::session::ParallelBranchNodeEnteredEvent,
        output: &Value,
    ) -> Result<(), ParallelTurnError> {
        let node = command
            .graph
            .nodes
            .iter()
            .find(|node| node.id == entered.work.node_id)
            .ok_or(ParallelTurnError::InvalidContribution)?;
        let output = output
            .as_object()
            .ok_or(ParallelTurnError::InvalidContribution)?;
        let shared = node
            .write_variables
            .iter()
            .filter_map(|name| {
                command
                    .graph
                    .variables
                    .iter()
                    .find(|declaration| {
                        declaration.name == *name
                            && matches!(
                                declaration.scope,
                                VariableScope::Run | VariableScope::Session
                            )
                            && declaration.merge_policy.is_some()
                    })
                    .map(|declaration| (name, declaration))
            })
            .collect::<Vec<_>>();
        for (name, declaration) in shared {
            let value = output
                .get(name)
                .ok_or(ParallelTurnError::InvalidContribution)
                .and_then(|value| {
                    canonical_value_from_json(value, &declaration.value_type)
                        .map_err(|_| ParallelTurnError::InvalidContribution)
                })?;
            self.commit_shared_contribution(command, entered, name, value)?;
        }
        Ok(())
    }

    fn commit_shared_contribution(
        &self,
        command: &DriveParallelTurnCommand,
        entered: &crate::session::ParallelBranchNodeEnteredEvent,
        variable: &str,
        value: CanonicalVariableValue,
    ) -> Result<(), ParallelTurnError> {
        let head = self.journal.load()?;
        let parallel =
            find_parallel(&head.state, &command.root_work).ok_or(ParallelTurnError::Projection)?;
        let branch = parallel
            .branches
            .get(&entered.branch_id)
            .ok_or(ParallelTurnError::Projection)?;
        let base_version = branch
            .region
            .variable_base_versions
            .get(variable)
            .copied()
            .ok_or(ParallelTurnError::InvalidContribution)
            .map(|version| if version == 0 { None } else { Some(version) })?;
        let value_hash = serde_json::to_vec(&value)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|_| ParallelTurnError::Serialization)?;
        let artifact_references = artifact_references(&value);
        let contribution = ParallelVariableContributionRecordedEvent {
            owner: command.root_work.clone(),
            branch_id: entered.branch_id.clone(),
            configured_member_reference: branch.region.member.configured_reference.clone(),
            stable_order: branch.region.member.branch_index,
            work: entered.work.clone(),
            executor: entered.executor.clone(),
            configuration_hash: entered.configuration_hash,
            variable: variable.to_owned(),
            base_version,
            value,
            value_hash,
            artifact_references: artifact_references.clone(),
        };
        if let Some(existing) = parallel
            .variable_contributions
            .get(variable)
            .and_then(|by_branch| by_branch.get(&entered.branch_id))
        {
            return if existing.contribution == contribution {
                Ok(())
            } else {
                Err(ParallelTurnError::ContributionConflict)
            };
        }
        let expected_artifacts = canonical_user_event_artifacts(&artifact_references)?;
        let artifacts = self
            .journal
            .resolve_artifacts(&artifact_references, &expected_artifacts)?;
        self.commit_payload(
            head,
            RuntimeCommittedEvent::ParallelVariableContributionRecorded(contribution),
            artifacts,
            None,
        )?;
        Ok(())
    }

    fn validate_existing_emit_receipt(
        &self,
        command: &DriveParallelTurnCommand,
        request: &BranchEffectRequest,
    ) -> Result<(), ParallelTurnError> {
        let head = self.journal.load()?;
        let variables = Self::branch_input(
            &head.state,
            &command.graph,
            &request.work,
            &command.variables,
        )?;
        let outcome = execute_native_emit_effect(&effect_command(
            command,
            request,
            active_branch_entry(&head.state, &request.work)
                .map(|active| branch_effect_identity(request, active, &variables))
                .transpose()?
                .ok_or(ParallelTurnError::Projection)?,
            variables,
            None,
        )?)
        .map_err(ParallelTurnError::EffectPort)?;
        let ParallelBranchEffectOutcome::Completed {
            emitted_event: Some(event),
            ..
        } = outcome
        else {
            return Err(ParallelTurnError::InvalidEffectOutcome);
        };
        self.validate_existing_emit_proposal(request, &event)
    }

    fn validate_existing_emit_proposal(
        &self,
        request: &BranchEffectRequest,
        event: &UserSpaceEventProposal,
    ) -> Result<(), ParallelTurnError> {
        let head = self.journal.load()?;
        let existing = head
            .state
            .style_execution
            .as_ref()
            .and_then(|execution| {
                execution
                    .emitted_user_events
                    .iter()
                    .find(|record| record.event.work == request.work)
            })
            .ok_or(ParallelTurnError::EffectReceiptConflict)?;
        let expected_artifacts = canonical_user_event_artifacts(&event.artifact_references)?;
        if existing.event.declared_event_type != event.declared_event_type
            || existing.event.payload != event.payload
            || existing.event.artifact_references != event.artifact_references
            || existing.event.metadata != event.metadata
            || existing.envelope_artifacts != expected_artifacts
        {
            return Err(ParallelTurnError::EffectReceiptConflict);
        }
        Ok(())
    }

    fn commit_branch_failure(
        &self,
        request: &BranchEffectRequest,
        code: &str,
    ) -> Result<(), ParallelTurnError> {
        let head = self.journal.load()?;
        let entered = active_branch_entry(&head.state, &request.work)
            .ok_or(ParallelTurnError::Projection)?
            .clone();
        if entered.branch_id != request.branch_id
            || entered.dispatch_id != request.dispatch_id
            || entered.executor != request.executor
        {
            return Err(ParallelTurnError::Projection);
        }
        self.commit_payload(
            head,
            RuntimeCommittedEvent::ParallelBranchNodeFailed(ParallelBranchNodeFailedEvent {
                entered,
                code: code.to_owned(),
            }),
            Vec::new(),
            None,
        )?;
        Ok(())
    }

    fn commit_branch_completion(
        &self,
        request: &BranchEffectRequest,
        output: crate::node_execution::NodeExecutionOutput,
        mut artifact_references: BTreeSet<String>,
    ) -> Result<(), ParallelTurnError> {
        let head = self.journal.load()?;
        let entered = active_branch_entry(&head.state, &request.work)
            .ok_or(ParallelTurnError::Projection)?
            .clone();
        if entered.branch_id != request.branch_id
            || entered.dispatch_id != request.dispatch_id
            || entered.executor != request.executor
        {
            return Err(ParallelTurnError::Projection);
        }
        artifact_references.extend(output.artifact_reference);
        self.commit_payload(
            head,
            RuntimeCommittedEvent::ParallelBranchNodeCompleted(ParallelBranchNodeCompletedEvent {
                entered,
                result: JoinMemberResult {
                    inline_value: Some(output.transition_variables),
                    node_result_reference: output.result_reference,
                    declared_artifact_references: artifact_references.clone(),
                    artifact_references,
                },
            }),
            Vec::new(),
            None,
        )?;
        Ok(())
    }

    fn complete_parallel_root(
        &self,
        command: &DriveParallelTurnCommand,
        parallel: &CanonicalParallelExecutionState,
    ) -> Result<(), ParallelTurnError> {
        let join_step = parallel
            .last_allocated_step
            .checked_add(1)
            .ok_or(ParallelTurnError::Budget)?;
        if join_step > command.max_steps {
            return Err(ParallelTurnError::Budget);
        }
        let root_result = format!(
            "parallel:{}",
            agentmod_primitives::ContentHash::digest(
                &serde_json::to_vec(&command.root_work)
                    .map_err(|_| ParallelTurnError::Serialization)?
            )
        );
        let head = self.journal.load()?;
        self.commit_payload(
            head,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: command.root_work.node_id.clone(),
                attempt: command.root_work.attempt,
                loop_iteration: command.root_work.loop_iteration,
                step: command.root_work.step,
                result_reference: Some(root_result),
                artifact_reference: None,
            }),
            Vec::new(),
            None,
        )?;
        Ok(())
    }

    fn drive_bound_join(
        &self,
        command: &DriveParallelTurnCommand,
        parallel: &CanonicalParallelExecutionState,
        join_work: &NodeWorkIdentity,
        join_executor: SessionNodeExecutorResolution,
    ) -> Result<Option<ParallelTurnOutcome>, ParallelTurnError> {
        let mut head = self.journal.load()?;
        let mut join = find_join(&head.state, join_work);
        if join.is_none() {
            let identity = self.journal.allocate_identity()?;
            let payload = initialize_join(&InitializeJoinDriverCommand {
                graph: command.graph.clone(),
                owner: join_work.clone(),
                executor: join_executor,
                parallel: parallel.clone(),
                timestamp: identity.timestamp,
            })?;
            head = self.commit_payload(head, payload, Vec::new(), Some(identity))?;
            join = find_join(&head.state, join_work);
        }
        let join = join.ok_or(ParallelTurnError::Projection)?.clone();
        let decision = match &join.lifecycle {
            GenericJoinLifecycleState::Ready(ready) => JoinDecision::Ready(ready.clone()),
            GenericJoinLifecycleState::Failed(failed)
            | GenericJoinLifecycleState::TimedOut(failed) => JoinDecision::Failed(failed.clone()),
            GenericJoinLifecycleState::Waiting => {
                let (decision, events) = drive_join(&DriveJoinCommand {
                    graph: command.graph.clone(),
                    parallel: parallel.clone(),
                    join,
                    timeout_elapsed: false,
                })?;
                if !events.is_empty() {
                    self.commit_payloads(events)?;
                }
                decision
            }
        };
        match decision {
            JoinDecision::Ready(ready) => {
                let result_reference = ready_join_result_reference(&ready)?;
                let merges = Self::ready_join_merges(command, parallel, &ready)?;
                self.effect_port
                    .apply_ready_join_merges(ApplyParallelJoinMergesCommand {
                        session_id: command.session_id,
                        graph: command.graph.clone(),
                        parallel_owner: parallel.owner.clone(),
                        join_work: join_work.clone(),
                        result_reference: result_reference.clone(),
                        merges: merges.clone(),
                    })
                    .map_err(ParallelTurnError::EffectPort)?;
                self.verify_ready_join_merges(command, join_work, &merges)?;
                self.complete_ready_join(join_work, result_reference)?;
                Ok(None)
            }
            JoinDecision::Failed(failure) => Ok(Some(ParallelTurnOutcome::JoinFailed {
                reason: format!("{:?}", failure.reason),
            })),
            JoinDecision::Waiting { .. } => Err(ParallelTurnError::JoinWaiting),
        }
    }

    fn ready_join_merges(
        command: &DriveParallelTurnCommand,
        parallel: &CanonicalParallelExecutionState,
        ready: &crate::parallel_execution::JoinReadyDescriptor,
    ) -> Result<Vec<ParallelJoinVariableMerge>, ParallelTurnError> {
        let successful = ready
            .results
            .iter()
            .map(|result| result.member_id.as_str())
            .collect::<BTreeSet<_>>();
        let mut merges = Vec::new();
        for declaration in command.graph.variables.iter().filter(|declaration| {
            matches!(
                declaration.scope,
                VariableScope::Run | VariableScope::Session
            ) && declaration.merge_policy.is_some()
        }) {
            let mut branches = Vec::new();
            let mut base_version = None;
            for member in parallel.execution.member_bindings() {
                if !successful.contains(member.configured_reference.as_str()) {
                    continue;
                }
                let branch = parallel
                    .branches
                    .get(&member.branch_id)
                    .ok_or(ParallelTurnError::Projection)?;
                if !branch.region.write_variables.contains(&declaration.name) {
                    continue;
                }
                let contribution = parallel
                    .variable_contributions
                    .get(&declaration.name)
                    .and_then(|by_branch| by_branch.get(&member.branch_id))
                    .ok_or(ParallelTurnError::MissingContribution)?;
                if contribution.contribution.work.node_id != declaration.producer
                    && !declaration
                        .merge_contributors
                        .contains(&contribution.contribution.work.node_id)
                {
                    return Err(ParallelTurnError::ContributionConflict);
                }
                let candidate_base = contribution.contribution.base_version;
                if !branches.is_empty() && base_version != candidate_base {
                    return Err(ParallelTurnError::ContributionConflict);
                }
                base_version = candidate_base;
                branches.push(BranchVariableValue {
                    branch_id: member.branch_id.clone(),
                    stable_order: member.branch_index,
                    value: contribution.contribution.value.clone(),
                });
            }
            if !branches.is_empty() {
                merges.push(ParallelJoinVariableMerge {
                    variable: declaration.name.clone(),
                    base_version,
                    branches,
                });
            }
        }
        Ok(merges)
    }

    fn verify_ready_join_merges(
        &self,
        command: &DriveParallelTurnCommand,
        join_work: &NodeWorkIdentity,
        merges: &[ParallelJoinVariableMerge],
    ) -> Result<(), ParallelTurnError> {
        if merges.is_empty() {
            return Ok(());
        }
        let head = self.journal.load()?;
        let variables = head
            .state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.canonical_variables.as_deref())
            .ok_or(ParallelTurnError::MergeNotCommitted)?;
        for merge in merges {
            let declaration = command
                .graph
                .variables
                .iter()
                .find(|declaration| declaration.name == merge.variable)
                .ok_or(ParallelTurnError::MergeNotCommitted)?;
            let expected = merge_branch_contributions(
                declaration
                    .merge_policy
                    .ok_or(ParallelTurnError::MergeNotCommitted)?,
                merge.branches.clone(),
            )
            .map_err(|_| ParallelTurnError::MergeNotCommitted)?;
            let entry = variables
                .environment()
                .canonical_entries()
                .get(&merge.variable)
                .ok_or(ParallelTurnError::MergeNotCommitted)?;
            if entry.version != merge.base_version.unwrap_or(0).saturating_add(1)
                || entry.value != expected
                || entry.writer
                    != (VariableWriter::Node {
                        node_id: join_work.node_id.clone(),
                        branch: None,
                    })
            {
                return Err(ParallelTurnError::MergeNotCommitted);
            }
        }
        Ok(())
    }

    fn complete_ready_join(
        &self,
        join_work: &NodeWorkIdentity,
        result_reference: String,
    ) -> Result<(), ParallelTurnError> {
        let head = self.journal.load()?;
        self.commit_payload(
            head,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: join_work.node_id.clone(),
                attempt: join_work.attempt,
                loop_iteration: join_work.loop_iteration,
                step: join_work.step,
                result_reference: Some(result_reference),
                artifact_reference: None,
            }),
            Vec::new(),
            None,
        )?;
        Ok(())
    }

    fn commit_payloads(
        &self,
        payloads: Vec<RuntimeCommittedEvent>,
    ) -> Result<ParallelTurnHead, ParallelTurnError> {
        let mut head = self.journal.load()?;
        for payload in payloads {
            head = self.commit_payload(head, payload, Vec::new(), None)?;
        }
        Ok(head)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "consuming the exact validated head prevents accidental reuse after a successful append"
    )]
    fn commit_payload(
        &self,
        head: ParallelTurnHead,
        payload: RuntimeCommittedEvent,
        artifacts: Vec<ArtifactReference>,
        identity: Option<ParallelTurnEventIdentity>,
    ) -> Result<ParallelTurnHead, ParallelTurnError> {
        let identity = identity.map_or_else(
            || self.journal.allocate_identity(),
            Result::<_, ParallelTurnJournalError>::Ok,
        )?;
        let sequence = head
            .state
            .last_sequence
            .checked_next()
            .map_err(|_| ParallelTurnError::Sequence)?;
        let event = seal_event(&head, sequence, identity, payload, artifacts)?;
        let next_state = reduce(Some(head.state.clone()), &event)?;
        self.journal.append(
            ParallelTurnAppendPosition {
                sequence: head.state.last_sequence,
                event_id: head.last_event_id,
            },
            event,
        )?;
        Ok(ParallelTurnHead {
            state: next_state,
            last_event_id: identity.event_id,
        })
    }
}

#[derive(Default)]
struct EffectProcessing {
    progressed: bool,
    waiting: Vec<EffectWaiting>,
}

struct EffectWaiting {
    branch_id: String,
    work: NodeWorkIdentity,
    continuation_reference: String,
}

fn execute_native_emit_effect(
    command: &ParallelBranchEffectCommand,
) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
    if command.request.kind != BranchEffectKind::EmitEvent {
        return Err(ParallelBranchEffectPortError {
            code: String::from("unsupported_effect"),
        });
    }
    let outcome = execute_native_node(&ExecuteNodeCommand {
        session_id: command.session_id,
        work: command.request.work.clone(),
        executor: command.request.executor.clone(),
        configuration: command.request.configuration.clone(),
        input: NodeExecutionInput {
            transition_variables: command.variables.clone(),
        },
        graph_state: CanonicalGraphState {
            attempt: command.request.work.attempt,
            loop_iteration: command.request.work.loop_iteration,
            step: command.request.work.step,
            completed_node_ids: Vec::new(),
        },
        budget_state: command.budget,
    })
    .map_err(|error| ParallelBranchEffectPortError {
        code: error.to_string(),
    })?;
    let NodeExecutionOutcome::Emitted { event, output } = outcome else {
        return Err(ParallelBranchEffectPortError {
            code: String::from("invalid_native_emit_outcome"),
        });
    };
    Ok(effect_outcome_with_hash(
        &command.identity,
        ParallelBranchEffectOutcome::Completed {
            output,
            artifact_references: BTreeSet::new(),
            receipt_hash: ContentHash::from_bytes([0; 32]),
            emitted_event: Some(Box::new(event)),
        },
    ))
}

fn branch_effect_identity(
    request: &BranchEffectRequest,
    active: &crate::session::ParallelBranchNodeEnteredEvent,
    variables: &Value,
) -> Result<ParallelBranchEffectIdentity, ParallelTurnError> {
    if active.work != request.work
        || active.branch_id != request.branch_id
        || active.dispatch_id != request.dispatch_id
        || active.executor != request.executor
    {
        return Err(ParallelTurnError::Projection);
    }
    Ok(ParallelBranchEffectIdentity {
        owner: active.owner.clone(),
        branch_id: request.branch_id.clone(),
        dispatch_id: request.dispatch_id.clone(),
        work: request.work.clone(),
        executor: request.executor.clone(),
        configuration_hash: active.configuration_hash,
        input_hash: ContentHash::digest(
            &serde_json::to_vec(variables).map_err(|_| ParallelTurnError::Serialization)?,
        ),
        effect_kind: branch_effect_kind_name(request)?.to_owned(),
    })
}

fn branch_effect_kind_name(
    request: &BranchEffectRequest,
) -> Result<&'static str, ParallelTurnError> {
    match request.dispatch_class()? {
        BranchEffectDispatchClass::Plugin => Ok("plugin"),
        BranchEffectDispatchClass::Runtime(kind) => Ok(match kind {
            BranchEffectKind::EmitEvent => "emit_event",
            BranchEffectKind::Delay => "delay",
            BranchEffectKind::Schedule => "schedule",
            BranchEffectKind::Tool => "tool",
            BranchEffectKind::Approval => "approval",
            BranchEffectKind::Child => "child",
            BranchEffectKind::OtherRuntimeEffect => "other_runtime_effect",
        }),
    }
}

fn branch_effect_record<'a>(
    state: &'a SessionState,
    request: &BranchEffectRequest,
) -> Option<&'a crate::session::ParallelBranchEffectRecord> {
    state
        .style_execution
        .as_ref()?
        .parallel_executions
        .values()
        .find(|parallel| {
            parallel
                .branches
                .get(&request.branch_id)
                .is_some_and(|branch| {
                    branch
                        .effect
                        .as_ref()
                        .is_some_and(|effect| effect.identity.work == request.work)
                })
        })?
        .branches
        .get(&request.branch_id)?
        .effect
        .as_ref()
}

fn validate_persisted_branch_effect_identity(
    request: &BranchEffectRequest,
    active: &crate::session::ParallelBranchNodeEnteredEvent,
    identity: &ParallelBranchEffectIdentity,
) -> Result<(), ParallelTurnError> {
    if identity.owner != active.owner
        || identity.branch_id != request.branch_id
        || identity.dispatch_id != request.dispatch_id
        || identity.work != request.work
        || identity.executor != request.executor
        || identity.configuration_hash != active.configuration_hash
        || identity.effect_kind != branch_effect_kind_name(request)?
        || active.work != request.work
        || active.branch_id != request.branch_id
        || active.dispatch_id != request.dispatch_id
        || active.executor != request.executor
    {
        return Err(ParallelTurnError::EffectReceiptConflict);
    }
    Ok(())
}

fn effect_command(
    command: &DriveParallelTurnCommand,
    request: &BranchEffectRequest,
    identity: ParallelBranchEffectIdentity,
    variables: Value,
    prior_outcome: Option<CanonicalBranchEffectOutcome>,
) -> Result<ParallelBranchEffectCommand, ParallelTurnError> {
    Ok(ParallelBranchEffectCommand {
        session_id: command.session_id,
        request: request.clone(),
        identity,
        variables,
        budget: branch_budget(command, request)?,
        prior_outcome,
    })
}

fn canonical_effect_outcome(outcome: &ParallelBranchEffectOutcome) -> CanonicalBranchEffectOutcome {
    match outcome {
        ParallelBranchEffectOutcome::Waiting {
            continuation_reference,
            receipt_hash,
        } => CanonicalBranchEffectOutcome::Waiting {
            continuation_reference: continuation_reference.clone(),
            receipt_hash: *receipt_hash,
        },
        ParallelBranchEffectOutcome::Completed {
            output,
            artifact_references,
            receipt_hash,
            ..
        } => CanonicalBranchEffectOutcome::Completed {
            output: ParallelBranchEffectOutput {
                result_reference: output.result_reference.clone(),
                artifact_reference: output.artifact_reference.clone(),
                artifact_references: output
                    .artifact_reference
                    .iter()
                    .cloned()
                    .chain(artifact_references.iter().cloned())
                    .collect(),
                transition_variables: output.transition_variables.clone(),
            },
            receipt_hash: *receipt_hash,
        },
        ParallelBranchEffectOutcome::Failed { code, receipt_hash } => {
            CanonicalBranchEffectOutcome::Failed {
                code: code.clone(),
                receipt_hash: *receipt_hash,
            }
        }
        ParallelBranchEffectOutcome::Ambiguous { code, receipt_hash } => {
            CanonicalBranchEffectOutcome::Ambiguous {
                code: code.clone(),
                receipt_hash: *receipt_hash,
            }
        }
    }
}

pub(crate) fn effect_outcome_with_hash(
    identity: &ParallelBranchEffectIdentity,
    mut outcome: ParallelBranchEffectOutcome,
) -> ParallelBranchEffectOutcome {
    let canonical = canonical_effect_outcome(&outcome);
    let receipt_hash = parallel_branch_effect_receipt_hash(identity, &canonical);
    match &mut outcome {
        ParallelBranchEffectOutcome::Waiting {
            receipt_hash: hash, ..
        }
        | ParallelBranchEffectOutcome::Completed {
            receipt_hash: hash, ..
        }
        | ParallelBranchEffectOutcome::Failed {
            receipt_hash: hash, ..
        }
        | ParallelBranchEffectOutcome::Ambiguous {
            receipt_hash: hash, ..
        } => *hash = receipt_hash,
    }
    outcome
}

fn node_output_from_canonical(output: ParallelBranchEffectOutput) -> NodeExecutionOutput {
    NodeExecutionOutput {
        result_reference: output.result_reference,
        artifact_reference: output.artifact_reference,
        transition_variables: output.transition_variables,
    }
}

fn branch_budget(
    command: &DriveParallelTurnCommand,
    request: &BranchEffectRequest,
) -> Result<CanonicalBudgetState, ParallelTurnError> {
    let remaining_steps = command
        .max_steps
        .checked_sub(request.work.step)
        .and_then(|remaining| remaining.checked_add(1))
        .ok_or(ParallelTurnError::Budget)?;
    let max_iterations = command
        .graph
        .nodes
        .iter()
        .find(|node| node.id == request.work.node_id)
        .ok_or(ParallelTurnError::Projection)?
        .max_iterations;
    Ok(CanonicalBudgetState {
        max_steps: command.max_steps,
        remaining_steps,
        max_iterations,
        remaining_iterations: max_iterations
            .map(|limit| limit.saturating_sub(request.work.loop_iteration)),
    })
}

fn parallel_join_target(
    graph: &ExecutableGraph,
    parallel_node_id: &str,
) -> Result<String, ParallelTurnError> {
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == parallel_node_id)
        .ok_or(ParallelTurnError::Projection)?;
    let Some(agentmod_graph_engine::NodeConfiguration::ParallelBranch { join_target, .. }) =
        node.configuration.as_ref()
    else {
        return Err(ParallelTurnError::Projection);
    };
    Ok(join_target.clone())
}

fn join_destination(
    graph: &ExecutableGraph,
    join_node_id: &str,
) -> Result<String, ParallelTurnError> {
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == join_node_id)
        .ok_or(ParallelTurnError::Projection)?;
    let outgoing = graph
        .edges
        .iter()
        .filter(|edge| edge.from == node.index)
        .collect::<Vec<_>>();
    if outgoing.len() != 1 || outgoing[0].condition.is_some() {
        return Err(ParallelTurnError::Transition);
    }
    graph
        .nodes
        .get(outgoing[0].to)
        .map(|node| node.id.clone())
        .ok_or(ParallelTurnError::Transition)
}

fn find_parallel<'a>(
    state: &'a SessionState,
    owner: &NodeWorkIdentity,
) -> Option<&'a CanonicalParallelExecutionState> {
    state
        .style_execution
        .as_ref()?
        .parallel_executions
        .values()
        .find(|parallel| parallel.owner == *owner)
}

fn find_join<'a>(
    state: &'a SessionState,
    owner: &NodeWorkIdentity,
) -> Option<&'a crate::session::GenericJoinExecutionState> {
    state
        .style_execution
        .as_ref()?
        .generic_joins
        .values()
        .find(|join| join.owner == *owner)
}

fn active_branch_entry<'a>(
    state: &'a SessionState,
    work: &NodeWorkIdentity,
) -> Option<&'a crate::session::ParallelBranchNodeEnteredEvent> {
    state
        .style_execution
        .as_ref()?
        .parallel_executions
        .values()
        .flat_map(|parallel| parallel.branches.values())
        .find_map(|branch| match &branch.control {
            ParallelBranchControlState::Active(entered) if entered.work == *work => Some(entered),
            _ => None,
        })
}

fn seal_event(
    head: &ParallelTurnHead,
    sequence: Sequence,
    identity: ParallelTurnEventIdentity,
    payload: RuntimeCommittedEvent,
    artifacts: Vec<ArtifactReference>,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, ParallelTurnError> {
    let parent_graph_node_id = match &payload {
        RuntimeCommittedEvent::UserSpaceEventEmitted(emitted) => Some(emitted.work.node_id.clone()),
        _ => None,
    };
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
            artifacts,
            classification: EventClassification::Committed,
        },
        payload,
    )
    .map_err(|_| ParallelTurnError::Event)
}

/// Stable live parallel-turn failure.
#[derive(Debug, Error)]
pub enum ParallelTurnError {
    /// Canonical session or graph state did not bind the command.
    #[error("parallel turn projection does not match the immutable command")]
    Projection,
    /// Driver rejected canonical parallel/join state.
    #[error("parallel driver failed: {0}")]
    Driver(#[from] ParallelDriverError),
    /// Native node execution failed.
    #[error("parallel branch node execution failed: {0}")]
    Native(#[from] crate::node_execution::NativeNodeExecutionError),
    /// Pure reducer rejected a proposed event before append.
    #[error("parallel turn reducer rejected a proposed event: {0}")]
    Reducer(#[from] SessionReducerError),
    /// Journal boundary failed.
    #[error(transparent)]
    Journal(#[from] ParallelTurnJournalError),
    /// Unsupported effect was not dispatched.
    #[error("parallel branch node `{node_id}` uses unsupported effect {kind:?}")]
    UnsupportedBranchEffect {
        /// Exact compiled node.
        node_id: String,
        /// Typed boundary refused by the coordinator.
        kind: BranchEffectKind,
    },
    /// Injected effect adapter failed without a typed terminal receipt.
    #[error(transparent)]
    EffectPort(#[from] ParallelBranchEffectPortError),
    /// Existing effect receipt did not match the immutable node proposal.
    #[error("parallel branch effect receipt conflicts with the immutable proposal")]
    EffectReceiptConflict,
    /// A shared branch contribution was absent, malformed, or did not match
    /// its compiled declaration.
    #[error("parallel branch shared-variable contribution is invalid")]
    InvalidContribution,
    /// Replay already retained a different contribution for this exact branch
    /// and variable.
    #[error("parallel branch shared-variable contribution conflicts with replay")]
    ContributionConflict,
    /// A successful configured member omitted a shared contribution declared
    /// by its immutable branch region.
    #[error("parallel join is missing a required successful branch contribution")]
    MissingContribution,
    /// The join merge boundary returned without the exact canonical merged
    /// variable state visible in replay.
    #[error("parallel join canonical variable merge was not committed")]
    MergeNotCommitted,
    /// Native executor returned an outcome invalid for the selected effect.
    #[error("parallel branch effect returned an invalid outcome")]
    InvalidEffectOutcome,
    /// A projection produced neither repair, work, nor terminal readiness.
    #[error("parallel turn made no canonical progress")]
    NoProgress,
    /// Bound join is incomplete without a durable timeout receipt.
    #[error("generic join remains waiting for canonical member or timeout evidence")]
    JoinWaiting,
    /// Compiled join transition is missing or ambiguous.
    #[error("generic join transition is missing or ambiguous")]
    Transition,
    /// Effective graph-step budget is exhausted.
    #[error("parallel turn graph-step budget is exhausted")]
    Budget,
    /// Canonical sequence overflowed.
    #[error("parallel turn sequence overflow")]
    Sequence,
    /// Canonical identity material failed to serialize.
    #[error("parallel turn identity serialization failed")]
    Serialization,
    /// Canonical event envelope failed to seal.
    #[error("parallel turn event could not be sealed")]
    Event,
    /// Bounded coordination rounds were exhausted.
    #[error("parallel turn coordination round bound was exhausted")]
    RoundLimit,
    /// Fresh coordinator task was cancelled or panicked before returning a
    /// typed canonical result.
    #[error("parallel turn coordinator task failed")]
    CoordinatorTask,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    use agentmod_event_model::ArtifactReference;
    use agentmod_graph_engine::{
        CompilerLimits, ExecutableGraph, GraphCacheInputs, NodeKind, compile,
    };
    use agentmod_primitives::{ContentHash, EventId};
    use agentmod_session_style_sdk::BuiltInStyle;
    use uuid::Uuid;

    use crate::{
        canonical_variable_coordinator::{
            CanonicalVariableCoordinator, CoordinatedVariableOperation, NodeOutputCompleteness,
            PlanNodeOutputCommand,
        },
        session::{
            RuntimeCommittedEvent, SessionCreatedEvent, SessionExecutionPlan,
            SessionExecutionPlanCompilation, SessionNodeExecutorBoundary,
            SessionNodeExecutorResolution, SessionNodeExecutorSource, StyleExecutionContract,
            StyleExecutionInitializedEvent, StyleNodeEnteredEvent, replay,
        },
        style_executor::tests::binding,
    };

    use super::*;

    #[derive(Clone, Debug)]
    struct FailureCut {
        event_type: String,
        occurrence: usize,
        after_append: bool,
    }

    #[derive(Clone)]
    struct MockJournal {
        inner: Arc<MockJournalInner>,
    }

    struct MockJournalInner {
        events: Mutex<Vec<EventEnvelope<RuntimeCommittedEvent>>>,
        next_identity: AtomicU64,
        failure: Mutex<Option<FailureCut>>,
    }

    #[derive(Clone, Copy)]
    enum FakeEffectPlan {
        Completed,
        WaitingThenCompleted,
        Ambiguous,
    }

    #[derive(Clone)]
    struct FakeEffectPort {
        plan: FakeEffectPlan,
        dispatches: Arc<Mutex<Vec<ParallelBranchEffectCommand>>>,
        recoveries: Arc<Mutex<Vec<ParallelBranchEffectCommand>>>,
        cancellations: Arc<Mutex<Vec<(ParallelBranchEffectCommand, String)>>>,
    }

    #[derive(Clone)]
    struct PendingOutputPort {
        effects: FakeEffectPort,
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        cancelled: Arc<AtomicBool>,
        applications: Arc<AtomicU64>,
    }

    impl PendingOutputPort {
        fn new() -> Self {
            Self {
                effects: FakeEffectPort::new(FakeEffectPlan::Completed),
                started: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Notify::new()),
                cancelled: Arc::new(AtomicBool::new(false)),
                applications: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl FakeEffectPort {
        fn new(plan: FakeEffectPlan) -> Self {
            Self {
                plan,
                dispatches: Arc::new(Mutex::new(Vec::new())),
                recoveries: Arc::new(Mutex::new(Vec::new())),
                cancellations: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn dispatch_count(&self) -> usize {
            self.dispatches.lock().expect("dispatches").len()
        }

        fn recovery_count(&self) -> usize {
            self.recoveries.lock().expect("recoveries").len()
        }

        fn cancellation_count(&self) -> usize {
            self.cancellations.lock().expect("cancellations").len()
        }

        fn completed(command: &ParallelBranchEffectCommand) -> ParallelBranchEffectOutcome {
            effect_outcome_with_hash(
                &command.identity,
                ParallelBranchEffectOutcome::Completed {
                    output: NodeExecutionOutput {
                        result_reference: Some(format!("effect:{}", command.request.work.node_id)),
                        artifact_reference: None,
                        transition_variables: serde_json::json!({
                            "effect_node": command.request.work.node_id,
                        }),
                    },
                    artifact_references: BTreeSet::new(),
                    receipt_hash: ContentHash::from_bytes([0; 32]),
                    emitted_event: None,
                },
            )
        }

        fn waiting(command: &ParallelBranchEffectCommand) -> ParallelBranchEffectOutcome {
            effect_outcome_with_hash(
                &command.identity,
                ParallelBranchEffectOutcome::Waiting {
                    continuation_reference: format!(
                        "continuation:{}",
                        command.request.work.node_id
                    ),
                    receipt_hash: ContentHash::from_bytes([0; 32]),
                },
            )
        }

        fn ambiguous(command: &ParallelBranchEffectCommand) -> ParallelBranchEffectOutcome {
            effect_outcome_with_hash(
                &command.identity,
                ParallelBranchEffectOutcome::Ambiguous {
                    code: String::from("external_receipt_missing"),
                    receipt_hash: ContentHash::from_bytes([0; 32]),
                },
            )
        }
    }

    #[derive(Clone)]
    struct CanonicalMergePort {
        effects: FakeEffectPort,
        journal: MockJournal,
    }

    #[async_trait]
    impl ParallelBranchEffectPort for CanonicalMergePort {
        async fn dispatch(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.effects.dispatch(command).await
        }

        async fn recover(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.effects.recover(command).await
        }

        fn apply_completed_output(
            &self,
            command: ApplyParallelBranchEffectOutputCommand,
        ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
            if command.effect_output
                && command
                    .graph
                    .nodes
                    .iter()
                    .find(|node| node.id == command.request.work.node_id)
                    .is_some_and(|node| node.write_variables.contains("shared"))
            {
                let result = command
                    .output
                    .result_reference
                    .clone()
                    .ok_or_else(|| parallel_effect_port_error("missing_tool_result"))?;
                return Ok(NodeExecutionOutput {
                    result_reference: command.output.result_reference,
                    artifact_reference: command.output.artifact_reference,
                    transition_variables: serde_json::json!({"shared": result}),
                });
            }
            Ok(command.output)
        }

        fn apply_ready_join_merges(
            &self,
            command: ApplyParallelJoinMergesCommand,
        ) -> Result<(), ParallelBranchEffectPortError> {
            for merge in command.merges {
                let operation = CoordinatedVariableOperation::Merge {
                    variable: merge.variable.clone(),
                    expected_version: merge.base_version,
                    branches: merge.branches.clone(),
                };
                let head = self
                    .journal
                    .load()
                    .map_err(|_| parallel_effect_port_error("merge_load_failed"))?;
                let execution = head
                    .state
                    .style_execution
                    .as_ref()
                    .ok_or_else(|| parallel_effect_port_error("merge_execution_missing"))?;
                let replayed = execution
                    .canonical_variables
                    .as_deref()
                    .ok_or_else(|| parallel_effect_port_error("merge_variables_missing"))?;
                if replayed
                    .environment()
                    .canonical_entries()
                    .get(&merge.variable)
                    .is_some_and(|entry| {
                        entry.version == merge.base_version.unwrap_or(0).saturating_add(1)
                            && entry.writer
                                == (VariableWriter::Node {
                                    node_id: command.join_work.node_id.clone(),
                                    branch: None,
                                })
                    })
                {
                    continue;
                }
                let prepared =
                    CanonicalVariableCoordinator::new(replayed, &command.graph, &command.join_work)
                        .and_then(|coordinator| coordinator.prepare(&operation))
                        .map_err(|_| parallel_effect_port_error("merge_prepare_failed"))?;
                let sequence = head
                    .state
                    .last_sequence
                    .checked_next()
                    .map_err(|_| parallel_effect_port_error("merge_sequence_failed"))?;
                self.journal
                    .append(
                        ParallelTurnAppendPosition {
                            sequence: head.state.last_sequence,
                            event_id: head.last_event_id,
                        },
                        envelope(sequence.get(), prepared.payload),
                    )
                    .map_err(|_| parallel_effect_port_error("merge_append_failed"))?;
            }
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    struct SharedPureExecutor;

    #[async_trait]
    impl PureBranchExecutor for SharedPureExecutor {
        async fn execute(
            &self,
            command: ExecuteNodeCommand,
        ) -> Result<NodeExecutionOutcome, crate::parallel_driver::PureBranchExecutionError>
        {
            Ok(NodeExecutionOutcome::Completed {
                output: NodeExecutionOutput {
                    result_reference: Some(format!("pure:{}", command.work.node_id)),
                    artifact_reference: None,
                    transition_variables: serde_json::json!({
                        "shared": [command.work.node_id],
                    }),
                },
            })
        }
    }

    #[derive(Clone, Copy)]
    struct FreshBranchPureExecutor;

    #[async_trait]
    impl PureBranchExecutor for FreshBranchPureExecutor {
        async fn execute(
            &self,
            command: ExecuteNodeCommand,
        ) -> Result<NodeExecutionOutcome, crate::parallel_driver::PureBranchExecutionError>
        {
            let transition_variables = if command.work.node_id == "left-write" {
                serde_json::json!({"fresh": "canonical-fresh"})
            } else {
                serde_json::json!({})
            };
            Ok(NodeExecutionOutcome::Completed {
                output: NodeExecutionOutput {
                    result_reference: Some(format!("pure:{}", command.work.node_id)),
                    artifact_reference: None,
                    transition_variables,
                },
            })
        }
    }

    #[derive(Clone)]
    struct FreshBranchEffectPort {
        journal: MockJournal,
        dispatches: Arc<Mutex<Vec<ParallelBranchEffectCommand>>>,
        recoveries: Arc<Mutex<Vec<ParallelBranchEffectCommand>>>,
    }

    impl FreshBranchEffectPort {
        fn new(journal: MockJournal) -> Self {
            Self {
                journal,
                dispatches: Arc::new(Mutex::new(Vec::new())),
                recoveries: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn dispatches(&self) -> Vec<ParallelBranchEffectCommand> {
            self.dispatches.lock().expect("dispatches").clone()
        }

        fn recovery_count(&self) -> usize {
            self.recoveries.lock().expect("recoveries").len()
        }

        fn apply_pure_variables(
            &self,
            command: &ApplyParallelBranchEffectOutputCommand,
        ) -> Result<(), ParallelBranchEffectPortError> {
            let head = self
                .journal
                .load()
                .map_err(|_| parallel_effect_port_error("freshness_load_failed"))?;
            let replayed = head
                .state
                .style_execution
                .as_ref()
                .and_then(|execution| execution.canonical_variables.as_deref())
                .ok_or_else(|| parallel_effect_port_error("freshness_variables_missing"))?;
            let events =
                CanonicalVariableCoordinator::new(replayed, &command.graph, &command.request.work)
                    .and_then(|coordinator| {
                        coordinator.plan_node_output(&PlanNodeOutputCommand {
                            output: command.output.transition_variables.clone(),
                            completeness: NodeOutputCompleteness::RequireAll,
                            recorded_runtime_values: BTreeMap::new(),
                            branch: Some(command.branch.clone()),
                        })
                    })
                    .map_err(|_| parallel_effect_port_error("freshness_plan_failed"))?;
            for prepared in events {
                let head = self
                    .journal
                    .load()
                    .map_err(|_| parallel_effect_port_error("freshness_reload_failed"))?;
                let sequence = head
                    .state
                    .last_sequence
                    .checked_next()
                    .map_err(|_| parallel_effect_port_error("freshness_sequence_failed"))?;
                self.journal
                    .append(
                        ParallelTurnAppendPosition {
                            sequence: head.state.last_sequence,
                            event_id: head.last_event_id,
                        },
                        envelope(sequence.get(), prepared.payload),
                    )
                    .map_err(|_| parallel_effect_port_error("freshness_append_failed"))?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl ParallelBranchEffectPort for FreshBranchEffectPort {
        async fn dispatch(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.dispatches
                .lock()
                .expect("dispatches")
                .push(command.clone());
            Ok(effect_outcome_with_hash(
                &command.identity,
                ParallelBranchEffectOutcome::Completed {
                    output: NodeExecutionOutput {
                        result_reference: Some(String::from("effect:left-effect")),
                        artifact_reference: None,
                        transition_variables: serde_json::json!({}),
                    },
                    artifact_references: BTreeSet::new(),
                    receipt_hash: ContentHash::from_bytes([0; 32]),
                    emitted_event: None,
                },
            ))
        }

        async fn recover(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.recoveries.lock().expect("recoveries").push(command);
            Err(parallel_effect_port_error(
                "terminal_effect_must_not_be_recovered",
            ))
        }

        fn apply_completed_output(
            &self,
            command: ApplyParallelBranchEffectOutputCommand,
        ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
            if !command.effect_output {
                self.apply_pure_variables(&command)?;
            }
            Ok(command.output)
        }
    }

    #[async_trait]
    impl ParallelBranchEffectPort for FakeEffectPort {
        async fn dispatch(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.dispatches
                .lock()
                .expect("dispatches")
                .push(command.clone());
            Ok(match self.plan {
                FakeEffectPlan::Completed => Self::completed(&command),
                FakeEffectPlan::WaitingThenCompleted => Self::waiting(&command),
                FakeEffectPlan::Ambiguous => Self::ambiguous(&command),
            })
        }

        async fn recover(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            let recovery_count = {
                let mut recoveries = self.recoveries.lock().expect("recoveries");
                recoveries.push(command.clone());
                recoveries.len()
            };
            Ok(match self.plan {
                FakeEffectPlan::WaitingThenCompleted if recovery_count == 1 => {
                    Self::waiting(&command)
                }
                FakeEffectPlan::Completed | FakeEffectPlan::WaitingThenCompleted => {
                    Self::completed(&command)
                }
                FakeEffectPlan::Ambiguous => Self::ambiguous(&command),
            })
        }

        async fn cancel_plugin(
            &self,
            command: ParallelBranchEffectCommand,
            reason_code: String,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.cancellations
                .lock()
                .expect("cancellations")
                .push((command.clone(), reason_code));
            Ok(effect_outcome_with_hash(
                &command.identity,
                ParallelBranchEffectOutcome::Failed {
                    code: String::from("plugin_cancelled"),
                    receipt_hash: ContentHash::from_bytes([0; 32]),
                },
            ))
        }

        fn apply_completed_output(
            &self,
            command: ApplyParallelBranchEffectOutputCommand,
        ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
            let node = command
                .graph
                .nodes
                .iter()
                .find(|node| node.id == command.request.work.node_id)
                .ok_or_else(|| parallel_effect_port_error("branch_output_node_missing"))?;
            if !node.write_variables.is_empty() {
                return Err(parallel_effect_port_error(
                    "branch_output_application_unavailable",
                ));
            }
            Ok(command.output)
        }
    }

    #[async_trait]
    impl ParallelBranchEffectPort for PendingOutputPort {
        async fn dispatch(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.effects.dispatch(command).await
        }

        async fn recover(
            &self,
            command: ParallelBranchEffectCommand,
        ) -> Result<ParallelBranchEffectOutcome, ParallelBranchEffectPortError> {
            self.effects.recover(command).await
        }

        fn apply_completed_output(
            &self,
            command: ApplyParallelBranchEffectOutputCommand,
        ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
            self.effects.apply_completed_output(command)
        }

        async fn apply_completed_output_owned(
            &self,
            command: ApplyParallelBranchEffectOutputCommand,
        ) -> Result<NodeExecutionOutput, ParallelBranchEffectPortError> {
            struct PendingApplicationGuard<'a> {
                cancelled: &'a AtomicBool,
                completed: bool,
            }

            impl Drop for PendingApplicationGuard<'_> {
                fn drop(&mut self) {
                    if !self.completed {
                        self.cancelled.store(true, Ordering::Relaxed);
                    }
                }
            }

            let mut guard = PendingApplicationGuard {
                cancelled: &self.cancelled,
                completed: false,
            };
            self.started.notify_one();
            self.release.notified().await;
            self.applications.fetch_add(1, Ordering::Relaxed);
            let result = self.apply_completed_output(command);
            guard.completed = true;
            result
        }
    }

    impl MockJournal {
        fn new(events: Vec<EventEnvelope<RuntimeCommittedEvent>>) -> Self {
            Self {
                inner: Arc::new(MockJournalInner {
                    events: Mutex::new(events),
                    next_identity: AtomicU64::new(100),
                    failure: Mutex::new(None),
                }),
            }
        }

        fn fail_once(&self, event_type: &str, occurrence: usize, after_append: bool) {
            *self.inner.failure.lock().expect("failure") = Some(FailureCut {
                event_type: event_type.to_owned(),
                occurrence,
                after_append,
            });
        }

        fn event_count(&self, event_type: &str) -> usize {
            self.inner
                .events
                .lock()
                .expect("events")
                .iter()
                .filter(|event| event.metadata.event_type == event_type)
                .count()
        }

        fn event_types(&self) -> Vec<String> {
            self.inner
                .events
                .lock()
                .expect("events")
                .iter()
                .map(|event| event.metadata.event_type.clone())
                .collect()
        }

        fn canonical_payloads(&self) -> Vec<Value> {
            self.inner
                .events
                .lock()
                .expect("events")
                .iter()
                .map(|event| serde_json::to_value(&event.payload).expect("payload"))
                .collect()
        }

        fn events(&self) -> Vec<EventEnvelope<RuntimeCommittedEvent>> {
            self.inner.events.lock().expect("events").clone()
        }
    }

    impl ParallelTurnJournal for MockJournal {
        fn load(&self) -> Result<ParallelTurnHead, ParallelTurnJournalError> {
            let events = self.inner.events.lock().expect("events");
            let state = replay(&*events).map_err(|error| ParallelTurnJournalError {
                code: error.to_string(),
            })?;
            Ok(ParallelTurnHead {
                state,
                last_event_id: events.last().expect("seeded").metadata.event_id,
            })
        }

        fn allocate_identity(&self) -> Result<ParallelTurnEventIdentity, ParallelTurnJournalError> {
            let value = self.inner.next_identity.fetch_add(1, Ordering::SeqCst);
            Ok(ParallelTurnEventIdentity {
                event_id: EventId::from_uuid(Uuid::from_u128(u128::from(value))),
                timestamp: TimestampMillis::new(i64::try_from(value).expect("timestamp")),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(9)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(10)),
            })
        }

        fn append(
            &self,
            expected: ParallelTurnAppendPosition,
            event: EventEnvelope<RuntimeCommittedEvent>,
        ) -> Result<(), ParallelTurnJournalError> {
            let mut events = self.inner.events.lock().expect("events");
            let head = events.last().expect("seeded");
            if head.metadata.sequence != expected.sequence
                || head.metadata.event_id != expected.event_id
                || event.metadata.sequence.get() != events.len() as u64 + 1
            {
                return Err(ParallelTurnJournalError {
                    code: String::from("append_conflict"),
                });
            }
            let occurrence = events
                .iter()
                .filter(|existing| existing.metadata.event_type == event.metadata.event_type)
                .count()
                + 1;
            let cut = self
                .inner
                .failure
                .lock()
                .expect("failure")
                .as_ref()
                .is_some_and(|cut| {
                    cut.event_type == event.metadata.event_type && cut.occurrence == occurrence
                });
            let after_append = cut
                && self
                    .inner
                    .failure
                    .lock()
                    .expect("failure")
                    .as_ref()
                    .is_some_and(|cut| cut.after_append);
            if cut && !after_append {
                self.inner.failure.lock().expect("failure").take();
                return Err(ParallelTurnJournalError {
                    code: String::from("cut_before_append"),
                });
            }
            events.push(event);
            if after_append {
                self.inner.failure.lock().expect("failure").take();
                return Err(ParallelTurnJournalError {
                    code: String::from("cut_after_append"),
                });
            }
            Ok(())
        }

        fn resolve_artifacts(
            &self,
            declared: &BTreeSet<String>,
            expected: &[ArtifactReference],
        ) -> Result<Vec<ArtifactReference>, ParallelTurnJournalError> {
            if declared.is_empty() && expected.is_empty() {
                Ok(Vec::new())
            } else {
                Err(ParallelTurnJournalError {
                    code: String::from("artifact_unavailable"),
                })
            }
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(1))
    }

    fn right_branch_manifest(branch_kind: &str) -> &'static str {
        match branch_kind {
            "conditional" => {
                r#"
[[nodes]]
id = "right"
kind = "conditional_branch"
"#
            }
            "emit" => {
                r#"
[[nodes]]
id = "right"
kind = "emit_event"
configuration = { type = "emit_event", event_type = "user.branch_progress", payload = { status = "ready" }, artifact_references = [], metadata = { source = "parallel" } }
"#
            }
            "delay" => {
                r#"
[[nodes]]
id = "right"
kind = "delay"
configuration = { type = "delay", resolution = { kind = "duration", duration_ms = 10 }, cancellation = "cancel_continuation" }
"#
            }
            "approval" => {
                r#"
[[nodes]]
id = "right"
kind = "user_approval"
configuration = { type = "user_approval", action_summary = { kind = "static", value = "approve branch" } }
"#
            }
            "tool" => {
                r#"
[[nodes]]
id = "right"
kind = "tool_execution_gate"
tool = "filesystem.read"
configuration = { type = "tool_execution", arguments = { kind = "static", value = { path = "README.md" } } }
"#
            }
            "child" => {
                r#"
[[nodes]]
id = "right"
kind = "spawn_child_agent"
"#
            }
            "artifact" => {
                r#"
[[nodes]]
id = "right"
kind = "persist_artifact"
configuration = { type = "persist_artifact", content = { kind = "static_text", value = "branch result" }, mime_type = "text/plain", security = "private", retention = "session" }
"#
            }
            _ => panic!("branch kind"),
        }
    }

    fn graph_cache_inputs() -> GraphCacheInputs {
        GraphCacheInputs {
            plugin_set_hash: ContentHash::digest(b"plugins"),
            runtime_api_version: String::from("0.1.0"),
            capability_set: BTreeSet::from([
                String::from("agents"),
                String::from("approval"),
                String::from("artifacts"),
                String::from("context"),
                String::from("events"),
                String::from("model"),
                String::from("scheduling"),
                String::from("tools"),
            ]),
        }
    }

    fn compile_graph(branch_kind: &str, max_parallelism: u32) -> ExecutableGraph {
        let right = right_branch_manifest(branch_kind);
        compile(
            &format!(
                r#"
format_version = 1
entry = "fanout"
[budget]
max_steps = 100
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 10000
[declarations]
capabilities = ["agents", "approval", "artifacts", "context", "events", "model", "scheduling", "tools"]
events = ["user.branch_progress"]
tools = ["filesystem.read"]
providers = ["mock"]

[[nodes]]
id = "fanout"
kind = "parallel_branch"
configuration = {{ type = "parallel_branch", max_parallelism = {max_parallelism}, max_queue_depth = 2, join_target = "join", join_policy = "all" }}
[[nodes]]
id = "left"
kind = "conditional_branch"
{right}
[[nodes]]
id = "join"
kind = "join_results"
configuration = {{ type = "join_results", required = ["left-result", "right-result"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }}
[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "fanout"
to = "left"
label = "left-result"
[[edges]]
from = "fanout"
to = "right"
label = "right-result"
[[edges]]
from = "left"
to = "join"
[[edges]]
from = "right"
to = "join"
[[edges]]
from = "join"
to = "done"
"#
            ),
            &graph_cache_inputs(),
            CompilerLimits::default(),
        )
        .expect("compile graph")
    }

    fn compile_shared_graph(effectful: bool) -> ExecutableGraph {
        let (value_type, merge_policy, left, right) = if effectful {
            (
                r#"{ kind = "tool_result_reference" }"#,
                "first_branch",
                r#"kind = "tool_execution_gate"
tool = "filesystem.read"
write_variables = ["shared"]
configuration = { type = "tool_execution", arguments = { kind = "static", value = { path = "left" } } }"#,
                r#"kind = "tool_execution_gate"
tool = "filesystem.read"
write_variables = ["shared"]
configuration = { type = "tool_execution", arguments = { kind = "static", value = { path = "right" } } }"#,
            )
        } else {
            (
                r#"{ kind = "list", item_type = { kind = "string" }, max_items = 8 }"#,
                "append",
                r#"kind = "conditional_branch"
write_variables = ["shared"]"#,
                r#"kind = "conditional_branch"
write_variables = ["shared"]"#,
            )
        };
        compile(
            &format!(
                r#"
format_version = 1
entry = "fanout"
[budget]
max_steps = 100
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 10000
[declarations]
capabilities = ["agents", "tools"]
tools = ["filesystem.read"]

[[variables]]
name = "shared"
type = {value_type}
scope = "run"
producer = "left"
merge_contributors = ["right"]
consumers = ["done"]
mutability = "mutable"
merge_policy = "{merge_policy}"
max_size_bytes = 4096
security_classification = "internal"

[[nodes]]
id = "fanout"
kind = "parallel_branch"
configuration = {{ type = "parallel_branch", max_parallelism = 2, max_queue_depth = 2, join_target = "join", join_policy = "all", variable_merge_policies = {{ shared = "{merge_policy}" }} }}
[[nodes]]
id = "left"
{left}
[[nodes]]
id = "right"
{right}
[[nodes]]
id = "join"
kind = "join_results"
configuration = {{ type = "join_results", required = ["left-result", "right-result"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }}
[[nodes]]
id = "done"
kind = "conditional_branch"
read_variables = ["shared"]
[[nodes]]
id = "terminal"
kind = "complete_session"

[[edges]]
from = "fanout"
to = "left"
label = "left-result"
[[edges]]
from = "fanout"
to = "right"
label = "right-result"
[[edges]]
from = "left"
to = "join"
[[edges]]
from = "right"
to = "join"
[[edges]]
from = "join"
to = "done"
[[edges]]
from = "done"
to = "terminal"
"#
            ),
            &graph_cache_inputs(),
            CompilerLimits::default(),
        )
        .expect("compile shared graph")
    }

    fn compile_fresh_branch_input_graph() -> ExecutableGraph {
        compile(
            r#"
format_version = 1
entry = "fanout"
[budget]
max_steps = 100
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 10000
[declarations]
capabilities = ["agents", "tools"]
tools = ["filesystem.read"]

[[variables]]
name = "fresh"
type = { kind = "string" }
scope = "branch"
producer = "left-write"
consumers = ["left-effect"]
mutability = "immutable"
max_size_bytes = 128
security_classification = "internal"

[[nodes]]
id = "fanout"
kind = "parallel_branch"
configuration = { type = "parallel_branch", max_parallelism = 2, max_queue_depth = 2, join_target = "join", join_policy = "all" }
[[nodes]]
id = "left-write"
kind = "conditional_branch"
write_variables = ["fresh"]
[[nodes]]
id = "left-effect"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_variables = ["fresh"]
configuration = { type = "tool_execution", arguments = { kind = "static", value = { path = "README.md" } } }
[[nodes]]
id = "right"
kind = "conditional_branch"
[[nodes]]
id = "join"
kind = "join_results"
configuration = { type = "join_results", required = ["left-result", "right-result"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }
[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "fanout"
to = "left-write"
label = "left-result"
[[edges]]
from = "fanout"
to = "right"
label = "right-result"
[[edges]]
from = "left-write"
to = "left-effect"
[[edges]]
from = "left-effect"
to = "join"
[[edges]]
from = "right"
to = "join"
[[edges]]
from = "join"
to = "done"
"#,
            &graph_cache_inputs(),
            CompilerLimits::default(),
        )
        .expect("compile fresh branch input graph")
    }

    fn executor_identity(kind: NodeKind) -> (&'static str, &'static str) {
        match kind {
            NodeKind::ParallelBranch => ("runtime.parallel", "parallel_branch"),
            NodeKind::ConditionalBranch => ("runtime.conditional", "conditional_branch"),
            NodeKind::JoinResults => ("runtime.join", "join_results"),
            NodeKind::EmitEvent => ("runtime.event-emission", "emit_event"),
            NodeKind::Delay => ("runtime.delay", "delay"),
            NodeKind::UserApproval => ("runtime.user-approval", "user_approval"),
            NodeKind::ToolExecutionGate => ("runtime.tool-gate", "tool_execution_gate"),
            NodeKind::SpawnChildAgent => ("runtime.child-spawn", "spawn_child_agent"),
            NodeKind::PersistArtifact => ("runtime.artifact-persistence", "persist_artifact"),
            NodeKind::CompleteSession => ("runtime.session-completion", "complete_session"),
            _ => panic!("fixture node kind"),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture constructs one complete immutable contract and canonical replay head for recovery tests"
    )]
    fn seeded(
        branch_kind: &str,
        max_parallelism: u32,
        max_steps: u64,
    ) -> (MockJournal, DriveParallelTurnCommand) {
        let graph = compile_graph(branch_kind, max_parallelism);
        seeded_graph(graph, max_steps)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the test fixture keeps the immutable arbitrary-graph binding, exact persisted executor plan, and replay seed visibly co-located"
    )]
    fn seeded_graph(
        graph: ExecutableGraph,
        max_steps: u64,
    ) -> (MockJournal, DriveParallelTurnCommand) {
        seeded_graph_with_plugin(graph, max_steps, None)
    }

    fn seeded_plugin_graph(
        graph: ExecutableGraph,
        max_steps: u64,
        plugin_node_id: &str,
    ) -> (MockJournal, DriveParallelTurnCommand) {
        seeded_graph_with_plugin(graph, max_steps, Some(plugin_node_id))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the test fixture keeps the immutable arbitrary-graph binding, optional plugin resolution, and replay seed visibly co-located"
    )]
    fn seeded_graph_with_plugin(
        mut graph: ExecutableGraph,
        max_steps: u64,
        plugin_node_id: Option<&str>,
    ) -> (MockJournal, DriveParallelTurnCommand) {
        if plugin_node_id.is_some() {
            graph
                .declarations
                .plugins
                .insert(String::from("fixture.plugin"));
        }
        let mut style_binding = binding(BuiltInStyle::DeclarativeGraph);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&style_binding.compiled_style_json).expect("compiled");
        compiled.style_id = String::from("user-parallel");
        compiled.graph = graph.clone();
        if plugin_node_id.is_some()
            && !compiled
                .allowed_plugins
                .iter()
                .any(|plugin| plugin == "fixture.plugin")
        {
            compiled
                .allowed_plugins
                .push(String::from("fixture.plugin"));
            compiled.allowed_plugins.sort();
        }
        style_binding.id = compiled.style_id.clone();
        style_binding.source_locator = String::from("fixture:user-parallel");
        style_binding.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled json");
        style_binding.compiled_style_hash =
            ContentHash::digest(style_binding.compiled_style_json.as_bytes());
        let mut resolutions = graph
            .nodes
            .iter()
            .map(|node| {
                let (executor_id, node_kind) = executor_identity(node.kind);
                let mut resolution = SessionNodeExecutorResolution {
                    node_id: node.id.clone(),
                    node_kind: node_kind.to_owned(),
                    executor_id: executor_id.to_owned(),
                    executor_version: String::from("1.0.0"),
                    source: SessionNodeExecutorSource::Runtime,
                    boundary: SessionNodeExecutorBoundary::RuntimeLogic,
                    required_capabilities: node.required_capabilities.iter().cloned().collect(),
                    resolved_capabilities: node.required_capabilities.iter().cloned().collect(),
                    runtime_api_requirement: String::from("^0.1"),
                    executor_declaration_hash: ContentHash::digest(executor_id.as_bytes()),
                    adapter_configuration_reference: ContentHash::digest(
                        &serde_json::to_vec(node).expect("node"),
                    ),
                };
                if plugin_node_id == Some(node.id.as_str()) {
                    resolution.executor_id = String::from("fixture.plugin-executor");
                    resolution.executor_version = String::from("3.2.1");
                    resolution.source = SessionNodeExecutorSource::Plugin {
                        plugin_id: String::from("fixture.plugin"),
                    };
                    resolution.boundary = SessionNodeExecutorBoundary::PluginHost;
                    resolution.executor_declaration_hash =
                        ContentHash::digest(b"fixture-plugin-declaration");
                }
                resolution
            })
            .collect::<Vec<_>>();
        resolutions.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let plan = SessionExecutionPlan {
            registry_hash: ContentHash::digest(b"fixture-parallel-registry"),
            compilation: SessionExecutionPlanCompilation {
                compiler: String::from("runtime-node-plan-v1"),
                compiled_style_hash: style_binding.compiled_style_hash,
                compiled_cache_key: style_binding.compiled_cache_key,
                runtime_api_version: style_binding.runtime_api_version.clone(),
            },
            nodes: resolutions.clone(),
        };
        let plan_hash =
            ContentHash::digest(&serde_json::to_vec(&plan).expect("plan serialization"));
        style_binding.execution_plan = Some(plan);
        style_binding.execution_plan_hash = Some(plan_hash);
        let contract = StyleExecutionContract {
            style_binding_hash: ContentHash::digest(
                &serde_json::to_vec(&style_binding).expect("binding"),
            ),
            execution_plan_hash: plan_hash,
            registry_hash: ContentHash::digest(b"fixture-parallel-registry"),
            node_executors: resolutions,
            initial_node_id: String::from("fanout"),
            initial_variables_json: if graph.variables.is_empty() {
                String::from(r#"{"route":"both"}"#)
            } else {
                String::from("{}")
            },
            invocation_provider: Some(String::from("mock")),
            invocation_model: Some(String::from("mock-model")),
            invocation_options_json: None,
            initial_budgets: style_binding.budgets,
            run_id: format!("style-run:{}", session_id()),
        };
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
                        graph: Box::new(graph.clone()),
                        input_reference: None,
                        execution_contract: Some(Box::new(contract.clone())),
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("fanout"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
        ];
        let root_executor = contract
            .node_executors
            .iter()
            .find(|resolution| resolution.node_id == "fanout")
            .expect("parallel executor")
            .clone();
        (
            MockJournal::new(events),
            DriveParallelTurnCommand {
                session_id: session_id(),
                graph,
                contract,
                root_work: NodeWorkIdentity {
                    run_id: format!("style-run:{}", session_id()),
                    node_id: String::from("fanout"),
                    branch_path: Vec::new(),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                },
                root_executor,
                variables: serde_json::json!({"route":"both"}),
                max_steps,
                cancellation_code: None,
            },
        )
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(u128::from(sequence) + 1)),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(i64::try_from(sequence).expect("timestamp")),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(9)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(10)),
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

    #[tokio::test]
    async fn pure_user_graph_advances_through_parallel_and_exact_join() {
        let (journal, command) = seeded("conditional", 2, 100);
        let result = ParallelTurnCoordinator::new(journal.clone())
            .drive(command)
            .await
            .expect("parallel turn");
        assert_eq!(
            result.outcome,
            ParallelTurnOutcome::Advanced {
                node_id: String::from("done"),
                step: 7,
            }
        );
        let state = journal.load().expect("head").state;
        let execution = state.style_execution.expect("execution");
        assert_eq!(execution.parallel_executions.len(), 1);
        assert_eq!(execution.generic_joins.len(), 1);
        assert!(matches!(
            execution
                .generic_joins
                .values()
                .next()
                .expect("join")
                .lifecycle,
            GenericJoinLifecycleState::Ready(_)
        ));
        assert_eq!(journal.event_count("graph.parallel_branch_dispatched"), 2);
        assert_eq!(journal.event_count("graph.parallel_branch_terminated"), 2);
    }

    #[tokio::test]
    async fn every_outer_control_prefix_recovers_without_duplicate_completion() {
        for (event_type, occurrence) in [
            ("style.node_completed", 1),
            ("style.transition_selected", 1),
            ("style.node_entered", 2),
            ("style.node_completed", 2),
            ("style.transition_selected", 2),
            ("style.node_entered", 3),
        ] {
            let (journal, command) = seeded("conditional", 2, 100);
            journal.fail_once(event_type, occurrence, true);
            assert!(
                ParallelTurnCoordinator::new(journal.clone())
                    .drive(command.clone())
                    .await
                    .is_err()
            );
            let result = ParallelTurnCoordinator::new(journal.clone())
                .drive(command)
                .await
                .expect("restart recovery");
            assert!(matches!(
                result.outcome,
                ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
            ));
            assert_eq!(journal.event_count("style.node_completed"), 2);
            assert_eq!(journal.event_count("style.transition_selected"), 2);
            assert_eq!(journal.event_count("style.node_entered"), 3);
        }
    }

    #[tokio::test]
    async fn append_conflict_before_commit_reloads_without_duplicate_control_event() {
        let (journal, command) = seeded("conditional", 2, 100);
        journal.fail_once("graph.parallel_branch_started", 1, false);
        assert!(matches!(
            ParallelTurnCoordinator::new(journal.clone())
                .drive(command.clone())
                .await,
            Err(ParallelTurnError::Journal(ref error)) if error.code == "cut_before_append"
        ));
        assert_eq!(journal.event_count("graph.parallel_branch_started"), 0);

        let result = ParallelTurnCoordinator::new(journal.clone())
            .drive(command)
            .await
            .expect("append-conflict reload");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
        ));
        assert_eq!(journal.event_count("graph.parallel_branch_started"), 2);
        assert_eq!(journal.event_count("graph.parallel_branch_terminated"), 2);
    }

    #[test]
    fn stale_same_sequence_with_substituted_head_identity_is_an_append_conflict() {
        let (journal, _) = seeded("conditional", 2, 100);
        let event = envelope(
            4,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: String::from("fanout"),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
                result_reference: Some(String::from("stale")),
                artifact_reference: None,
            }),
        );
        let error = journal
            .append(
                ParallelTurnAppendPosition {
                    sequence: Sequence::new(3).expect("sequence"),
                    event_id: EventId::from_uuid(Uuid::from_u128(9_999)),
                },
                event,
            )
            .expect_err("substituted head must lose the append race");
        assert_eq!(error.code, "append_conflict");
        assert_eq!(journal.event_types().len(), 3);
    }

    #[tokio::test]
    async fn emitted_effect_receipt_repairs_after_ambiguous_append_without_redispatch() {
        let (journal, command) = seeded("emit", 2, 100);
        journal.fail_once("graph.user_space_event_emitted", 1, true);
        assert!(
            ParallelTurnCoordinator::new(journal.clone())
                .drive(command.clone())
                .await
                .is_err()
        );
        assert_eq!(journal.event_count("graph.user_space_event_emitted"), 1);
        let result = ParallelTurnCoordinator::new(journal.clone())
            .drive(command)
            .await
            .expect("receipt recovery");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
        ));
        assert_eq!(journal.event_count("graph.user_space_event_emitted"), 1);
        assert_eq!(
            journal.event_count("graph.parallel_branch_node_completed"),
            2
        );
    }

    #[tokio::test]
    async fn budget_exhaustion_commits_no_partial_dispatch_batch() {
        let (journal, command) = seeded("conditional", 2, 2);
        assert!(matches!(
            ParallelTurnCoordinator::new(journal.clone())
                .drive(command)
                .await,
            Err(ParallelTurnError::Driver(ParallelDriverError::Budget))
        ));
        assert_eq!(journal.event_count("graph.parallel_branch_dispatched"), 0);
    }

    #[tokio::test]
    async fn cancellation_is_canonical_and_duplicate_suppressed() {
        let (journal, mut command) = seeded("conditional", 2, 100);
        command.cancellation_code = Some(String::from("user_cancelled"));
        for _ in 0..2 {
            let result = ParallelTurnCoordinator::new(journal.clone())
                .drive(command.clone())
                .await
                .expect("cancel");
            assert_eq!(result.outcome, ParallelTurnOutcome::Cancelled);
        }
        assert_eq!(
            journal.event_count("graph.parallel_cancellation_requested"),
            1
        );
        assert_eq!(
            journal.event_count("graph.parallel_cancellation_completed"),
            1
        );
        assert_eq!(journal.event_count("graph.parallel_branch_dispatched"), 0);
    }

    #[tokio::test]
    async fn injected_effect_port_recovers_delay_approval_tool_child_and_artifact_without_redispatch()
     {
        for branch_kind in ["delay", "approval", "tool", "child", "artifact"] {
            let (journal, command) = seeded(branch_kind, 2, 100);
            let port = FakeEffectPort::new(FakeEffectPlan::Completed);
            journal.fail_once("graph.parallel_branch_effect_outcome_recorded", 1, true);
            assert!(
                ParallelTurnCoordinator::with_ports(
                    journal.clone(),
                    Arc::new(NativePureBranchExecutor),
                    Arc::new(port.clone()),
                )
                .drive(command.clone())
                .await
                .is_err(),
                "{branch_kind} crash cut"
            );
            assert_eq!(port.dispatch_count(), 1, "{branch_kind} dispatched once");

            let result = ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(NativePureBranchExecutor),
                Arc::new(port.clone()),
            )
            .drive(command)
            .await
            .expect("terminal receipt recovery");
            assert!(
                matches!(
                    result.outcome,
                    ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
                ),
                "{branch_kind} advances"
            );
            assert_eq!(port.dispatch_count(), 1, "{branch_kind} not redispatched");
            assert_eq!(
                journal.event_count("graph.parallel_branch_effect_dispatched"),
                1
            );
            assert_eq!(
                journal.event_count("graph.parallel_branch_effect_outcome_recorded"),
                1
            );
        }
    }

    #[tokio::test]
    async fn plugin_branch_crash_recovers_exact_identity_without_redispatch() {
        let (journal, command) = seeded_plugin_graph(compile_graph("delay", 2), 100, "right");
        let port = FakeEffectPort::new(FakeEffectPlan::Completed);
        journal.fail_once("graph.parallel_branch_effect_outcome_recorded", 1, true);
        assert!(
            ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(NativePureBranchExecutor),
                Arc::new(port.clone()),
            )
            .drive(command.clone())
            .await
            .is_err()
        );
        assert_eq!(port.dispatch_count(), 1);
        let dispatched = port.dispatches.lock().expect("dispatches")[0].clone();
        assert_eq!(
            dispatched.request.dispatch_class().expect("plugin class"),
            BranchEffectDispatchClass::Plugin
        );
        assert_eq!(dispatched.identity.effect_kind, "plugin");
        let branch = dispatched.plugin_branch_context().expect("branch context");
        assert_eq!(branch.branch_id, dispatched.request.branch_id);
        assert_eq!(branch.stable_order, dispatched.request.stable_order);
        assert!(!branch.serialized_shared_write);
        let mut substituted = dispatched.clone();
        substituted.variables["substituted"] = serde_json::json!(true);
        assert!(substituted.plugin_branch_context().is_err());

        let result = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(NativePureBranchExecutor),
            Arc::new(port.clone()),
        )
        .drive(command)
        .await
        .expect("terminal plugin receipt recovery");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
        ));
        assert_eq!(port.dispatch_count(), 1);
        assert_eq!(port.recovery_count(), 0);
        assert_eq!(
            journal.event_count("graph.parallel_branch_effect_dispatched"),
            1
        );
        assert_eq!(
            journal.event_count("graph.parallel_branch_effect_outcome_recorded"),
            1
        );
    }

    #[tokio::test]
    async fn waiting_plugin_cancellation_is_terminal_receipt_driven_and_idempotent() {
        let (journal, command) = seeded_plugin_graph(compile_graph("delay", 2), 100, "right");
        let port = FakeEffectPort::new(FakeEffectPlan::WaitingThenCompleted);
        let waiting = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(NativePureBranchExecutor),
            Arc::new(port.clone()),
        )
        .drive(command.clone())
        .await
        .expect("plugin waiting");
        assert!(matches!(
            waiting.outcome,
            ParallelTurnOutcome::Waiting { .. }
        ));
        assert_eq!(port.dispatch_count(), 1);

        let mut cancelled = command.clone();
        cancelled.cancellation_code = Some(String::from("parent_cancelled"));
        for _ in 0..2 {
            let result = ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(NativePureBranchExecutor),
                Arc::new(port.clone()),
            )
            .drive(cancelled.clone())
            .await
            .expect("plugin cancellation");
            assert_eq!(result.outcome, ParallelTurnOutcome::Cancelled);
        }
        assert_eq!(port.dispatch_count(), 1);
        assert_eq!(port.cancellation_count(), 1);
        assert_eq!(
            journal.event_count("graph.parallel_branch_effect_outcome_recorded"),
            2
        );
        assert_eq!(
            journal.event_count("graph.parallel_cancellation_completed"),
            1
        );
        let event_types = journal.event_types();
        let requested = event_types
            .iter()
            .position(|event| event == "graph.parallel_cancellation_requested")
            .expect("canonical cancellation request");
        let terminal_receipt = event_types
            .iter()
            .rposition(|event| event == "graph.parallel_branch_effect_outcome_recorded")
            .expect("terminal plugin cancellation receipt");
        let completed = event_types
            .iter()
            .position(|event| event == "graph.parallel_cancellation_completed")
            .expect("canonical cancellation completion");
        assert!(requested < terminal_receipt);
        assert!(terminal_receipt < completed);
    }

    #[tokio::test]
    async fn dropping_parallel_drive_aborts_pending_output_application_without_late_events() {
        let (journal, command) = seeded("delay", 2, 100);
        let port = PendingOutputPort::new();
        let coordinator = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(NativePureBranchExecutor),
            Arc::new(port.clone()),
        );
        let drive = tokio::spawn(async move { coordinator.drive(command).await });

        tokio::time::timeout(std::time::Duration::from_secs(1), port.started.notified())
            .await
            .expect("output application started");
        let before_drop = journal.event_types();
        drive.abort();
        assert!(
            drive
                .await
                .expect_err("parallel drive must be cancelled")
                .is_cancelled()
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !port.cancelled.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending output application aborted");

        port.release.notify_waiters();
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert_eq!(port.applications.load(Ordering::Relaxed), 0);
        assert_eq!(journal.event_types(), before_drop);
    }

    #[tokio::test]
    async fn dispatch_intent_crash_recovers_through_reconcile_only() {
        let (journal, command) = seeded("delay", 2, 100);
        let port = FakeEffectPort::new(FakeEffectPlan::Completed);
        journal.fail_once("graph.parallel_branch_effect_dispatched", 1, true);
        assert!(
            ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(NativePureBranchExecutor),
                Arc::new(port.clone()),
            )
            .drive(command.clone())
            .await
            .is_err()
        );
        assert_eq!(port.dispatch_count(), 0);

        let result = ParallelTurnCoordinator::with_ports(
            journal,
            Arc::new(NativePureBranchExecutor),
            Arc::new(port.clone()),
        )
        .drive(command)
        .await
        .expect("reconcile existing intent");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::Advanced { .. }
        ));
        assert_eq!(port.dispatch_count(), 0);
        assert_eq!(port.recovery_count(), 1);
    }

    #[tokio::test]
    async fn waiting_effect_is_normal_and_cancellation_suppresses_recovery_and_later_dispatch() {
        let (journal, command) = seeded("approval", 2, 100);
        let port = FakeEffectPort::new(FakeEffectPlan::WaitingThenCompleted);
        let waiting = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(NativePureBranchExecutor),
            Arc::new(port.clone()),
        )
        .drive(command.clone())
        .await
        .expect("waiting is a normal outcome");
        assert!(matches!(
            waiting.outcome,
            ParallelTurnOutcome::Waiting {
                ref continuation_reference,
                ..
            } if continuation_reference == "continuation:right"
        ));
        assert_eq!(port.dispatch_count(), 1);
        assert_eq!(port.recovery_count(), 1);

        let mut cancelled = command;
        cancelled.cancellation_code = Some(String::from("user_cancelled"));
        let result = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(NativePureBranchExecutor),
            Arc::new(port.clone()),
        )
        .drive(cancelled)
        .await
        .expect("cancellation");
        assert_eq!(result.outcome, ParallelTurnOutcome::Cancelled);
        assert_eq!(port.dispatch_count(), 1);
        assert_eq!(port.recovery_count(), 1);
    }

    #[tokio::test]
    async fn ambiguous_external_effect_is_canonical_and_never_redispatched() {
        let (journal, command) = seeded("tool", 2, 100);
        let port = FakeEffectPort::new(FakeEffectPlan::Ambiguous);
        journal.fail_once("graph.parallel_branch_effect_outcome_recorded", 1, true);
        assert!(
            ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(NativePureBranchExecutor),
                Arc::new(port.clone()),
            )
            .drive(command.clone())
            .await
            .is_err()
        );
        let result = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(NativePureBranchExecutor),
            Arc::new(port.clone()),
        )
        .drive(command)
        .await
        .expect("ambiguous receipt terminalizes without retry");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::JoinFailed { .. }
        ));
        assert_eq!(port.dispatch_count(), 1);
        assert_eq!(port.recovery_count(), 0);
        assert_eq!(
            journal.event_count("graph.parallel_branch_effect_outcome_recorded"),
            1
        );
    }

    #[tokio::test]
    async fn replay_rejects_effect_kind_and_receipt_body_substitution() {
        let (journal, command) = seeded("tool", 2, 100);
        let port = FakeEffectPort::new(FakeEffectPlan::Completed);
        journal.fail_once("graph.parallel_branch_effect_outcome_recorded", 1, true);
        assert!(
            ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(NativePureBranchExecutor),
                Arc::new(port),
            )
            .drive(command)
            .await
            .is_err()
        );

        let mut kind_events = journal.events();
        let kind_index = kind_events
            .iter()
            .position(|event| {
                matches!(
                    event.payload,
                    RuntimeCommittedEvent::ParallelBranchEffectDispatched(_)
                )
            })
            .expect("dispatch event");
        let RuntimeCommittedEvent::ParallelBranchEffectDispatched(mut dispatched) =
            kind_events[kind_index].payload.clone()
        else {
            unreachable!("dispatch event index")
        };
        dispatched.identity.effect_kind = String::from("delay");
        let sequence = kind_events[kind_index].metadata.sequence.get();
        kind_events[kind_index] = envelope(
            sequence,
            RuntimeCommittedEvent::ParallelBranchEffectDispatched(dispatched),
        );
        assert!(replay(&kind_events).is_err());

        let mut input_events = journal.events();
        let RuntimeCommittedEvent::ParallelBranchEffectDispatched(mut dispatched) =
            input_events[kind_index].payload.clone()
        else {
            unreachable!("dispatch event index")
        };
        dispatched.identity.input_hash = ContentHash::digest(b"substituted-input");
        input_events[kind_index] = envelope(
            sequence,
            RuntimeCommittedEvent::ParallelBranchEffectDispatched(dispatched),
        );
        assert!(replay(&input_events).is_err());

        let mut receipt_events = journal.events();
        let receipt_index = receipt_events
            .iter()
            .position(|event| {
                matches!(
                    event.payload,
                    RuntimeCommittedEvent::ParallelBranchEffectOutcomeRecorded(_)
                )
            })
            .expect("outcome event");
        let RuntimeCommittedEvent::ParallelBranchEffectOutcomeRecorded(mut recorded) =
            receipt_events[receipt_index].payload.clone()
        else {
            unreachable!("outcome event index")
        };
        let CanonicalBranchEffectOutcome::Completed { output, .. } = &mut recorded.outcome else {
            panic!("completed outcome");
        };
        output.transition_variables = serde_json::json!({"substituted": true});
        let sequence = receipt_events[receipt_index].metadata.sequence.get();
        receipt_events[receipt_index] = envelope(
            sequence,
            RuntimeCommittedEvent::ParallelBranchEffectOutcomeRecorded(recorded),
        );
        assert!(replay(&receipt_events).is_err());

        let mut identity_events = journal.events();
        let RuntimeCommittedEvent::ParallelBranchEffectOutcomeRecorded(mut recorded) =
            identity_events[receipt_index].payload.clone()
        else {
            unreachable!("outcome event index")
        };
        recorded.identity.dispatch_id.push_str("-substituted");
        let receipt_hash =
            parallel_branch_effect_receipt_hash(&recorded.identity, &recorded.outcome);
        match &mut recorded.outcome {
            CanonicalBranchEffectOutcome::Waiting {
                receipt_hash: hash, ..
            }
            | CanonicalBranchEffectOutcome::Completed {
                receipt_hash: hash, ..
            }
            | CanonicalBranchEffectOutcome::Failed {
                receipt_hash: hash, ..
            }
            | CanonicalBranchEffectOutcome::Ambiguous {
                receipt_hash: hash, ..
            } => *hash = receipt_hash,
        }
        identity_events[receipt_index] = envelope(
            sequence,
            RuntimeCommittedEvent::ParallelBranchEffectOutcomeRecorded(recorded),
        );
        assert!(replay(&identity_events).is_err());
    }

    #[tokio::test]
    async fn unsupported_delay_branch_fails_closed_before_scheduler_effect() {
        let (journal, command) = seeded("delay", 2, 100);
        assert!(matches!(
            ParallelTurnCoordinator::new(journal.clone())
                .drive(command)
                .await,
            Err(ParallelTurnError::EffectPort(ParallelBranchEffectPortError {
                ref code
            })) if code == "unsupported_effect"
        ));
        assert_eq!(journal.event_count("graph.schedule_dispatched"), 0);
        assert_eq!(journal.event_count("graph.schedule_stored"), 0);
    }

    #[tokio::test]
    async fn event_order_is_stable_across_identical_runs() {
        let (left, left_command) = seeded("conditional", 2, 100);
        let (right, right_command) = seeded("conditional", 2, 100);
        ParallelTurnCoordinator::new(left.clone())
            .drive(left_command)
            .await
            .expect("left");
        ParallelTurnCoordinator::new(right.clone())
            .drive(right_command)
            .await
            .expect("right");
        assert_eq!(left.event_types(), right.event_types());
        assert_eq!(left.canonical_payloads(), right.canonical_payloads());
    }

    #[tokio::test]
    async fn pure_shared_contributions_merge_once_and_recover_before_node_completion() {
        let (journal, command) = seeded_graph(compile_shared_graph(false), 100);
        let port = CanonicalMergePort {
            effects: FakeEffectPort::new(FakeEffectPlan::Completed),
            journal: journal.clone(),
        };
        journal.fail_once("graph.parallel_variable_contribution_recorded", 1, true);
        assert!(
            ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(SharedPureExecutor),
                Arc::new(port.clone()),
            )
            .drive(command.clone())
            .await
            .is_err()
        );
        for index in 1..=journal.events().len() {
            if let Err(error) = replay(&journal.events()[..index]) {
                panic!(
                    "first invalid prefix {index} {}: {error:?}",
                    journal.events()[index - 1].metadata.event_type
                );
            }
        }
        let result = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(SharedPureExecutor),
            Arc::new(port),
        )
        .drive(command)
        .await
        .expect("restart completes exact merge");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
        ));
        assert_eq!(
            journal.event_count("graph.parallel_variable_contribution_recorded"),
            2
        );
        assert_eq!(journal.event_count("graph.variable_merged"), 1);
        let head = journal.load().expect("replay");
        let entry = head
            .state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.canonical_variables.as_deref())
            .and_then(|variables| variables.environment().canonical_entries().get("shared"))
            .expect("merged shared variable");
        assert_eq!(
            entry.value,
            CanonicalVariableValue::List(vec![
                CanonicalVariableValue::String(String::from("left")),
                CanonicalVariableValue::String(String::from("right")),
            ])
        );
    }

    #[tokio::test]
    async fn runtime_effect_slots_contribute_then_merge_without_redispatch_after_merge_cut() {
        let (journal, command) = seeded_graph(compile_shared_graph(true), 100);
        let effects = FakeEffectPort::new(FakeEffectPlan::Completed);
        let port = CanonicalMergePort {
            effects: effects.clone(),
            journal: journal.clone(),
        };
        journal.fail_once("graph.variable_merged", 1, true);
        assert!(
            ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(NativePureBranchExecutor),
                Arc::new(port.clone()),
            )
            .drive(command.clone())
            .await
            .is_err()
        );
        let result = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(NativePureBranchExecutor),
            Arc::new(port),
        )
        .drive(command)
        .await
        .expect("merge receipt recovery");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
        ));
        assert_eq!(effects.dispatch_count(), 2);
        assert_eq!(
            journal.event_count("graph.parallel_variable_contribution_recorded"),
            2
        );
        assert_eq!(journal.event_count("graph.variable_merged"), 1);
        let head = journal.load().expect("replay");
        let entry = head
            .state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.canonical_variables.as_deref())
            .and_then(|variables| variables.environment().canonical_entries().get("shared"))
            .expect("merged effect contribution");
        assert_eq!(
            entry.value,
            CanonicalVariableValue::ToolResultReference(String::from("effect:left"))
        );
    }

    #[tokio::test]
    async fn later_branch_effect_reads_replayed_pure_write_without_redispatch() {
        let (journal, mut command) = seeded_graph(compile_fresh_branch_input_graph(), 100);
        command.variables = serde_json::json!({"fresh": "stale-initial"});
        let port = FreshBranchEffectPort::new(journal.clone());
        journal.fail_once("graph.parallel_branch_effect_outcome_recorded", 1, true);

        assert!(
            ParallelTurnCoordinator::with_ports(
                journal.clone(),
                Arc::new(FreshBranchPureExecutor),
                Arc::new(port.clone()),
            )
            .drive(command.clone())
            .await
            .is_err()
        );

        let dispatches = port.dispatches();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(
            dispatches[0].variables,
            serde_json::json!({"fresh": "canonical-fresh"})
        );
        assert_eq!(
            dispatches[0].identity.input_hash,
            ContentHash::digest(
                &serde_json::to_vec(&serde_json::json!({"fresh": "canonical-fresh"}))
                    .expect("canonical input"),
            )
        );
        let replayed = journal.load().expect("restart replay").state;
        let branch_id = dispatches[0].request.branch_id.clone();
        let branch_entry = replayed
            .style_execution
            .as_ref()
            .and_then(|execution| execution.canonical_variables.as_deref())
            .and_then(|variables| variables.environment().canonical_entries().get("fresh"))
            .expect("canonical branch write survives replay");
        assert_eq!(branch_entry.branch_id.as_deref(), Some(branch_id.as_str()));
        assert_eq!(
            branch_entry.value,
            CanonicalVariableValue::String(String::from("canonical-fresh"))
        );

        let result = ParallelTurnCoordinator::with_ports(
            journal.clone(),
            Arc::new(FreshBranchPureExecutor),
            Arc::new(port.clone()),
        )
        .drive(command)
        .await
        .expect("restart completes from canonical effect receipt");
        assert!(matches!(
            result.outcome,
            ParallelTurnOutcome::Advanced { ref node_id, .. } if node_id == "done"
        ));
        assert_eq!(port.dispatches().len(), 1);
        assert_eq!(port.recovery_count(), 0);
        assert_eq!(
            journal.event_count("graph.parallel_branch_effect_outcome_recorded"),
            1
        );
    }
}
