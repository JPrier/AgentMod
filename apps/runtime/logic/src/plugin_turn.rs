//! Durable live coordination for exact plugin-provided graph-node executors.
//!
//! The coordinator persists the invocation outbox before crossing the isolated
//! plugin boundary. Replay is authoritative: a coordinator reconstructed at a
//! dispatched cut never calls the plugin automatically. It can only reduce an
//! exact durable terminal receipt or commit a fail-closed terminal
//! classification.

use std::path::PathBuf;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_event_pipeline::{ActionCapabilities, BlockingPipeline};
use agentmod_graph_engine::ExecutableGraph;
use agentmod_primitives::{
    CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId, TimestampMillis, Version,
};
use agentmod_runtime_data::{
    artifact::ArtifactDataPort,
    cancellation::{RuntimeCancellationDataPort, RuntimeCancellationDataRequest},
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
    plugin::{
        CancelPluginNodeInvocationDataRequest, PluginDataError, PluginDataPort,
        PluginInvocationCancellationDataResult, PluginInvocationCancellationDataStatus,
    },
    plugin_receipt::{
        PluginNodeReceiptDataError, PluginNodeReceiptDataIdentity, PluginNodeReceiptDataPort,
        StorePluginNodeReceiptDataRequest,
    },
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    action::{ActionProposal, ConsequentialAction, PluginNodeInvocationAction, ProposalId},
    canonical_variables::{
        BranchWriteContext, CanonicalVariableEventReducer, CanonicalVariableValue,
    },
    node_execution::NodeWorkIdentity,
    node_executor::{NodeExecutorBoundary, NodeExecutorSource, ResolvedNodeExecutor},
    permission::PermissionPolicy,
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort,
    },
    plugin::{
        ExecutePluginNodeCommand, PluginCompositionLogic, PluginInvocationCancellationTarget,
        PluginNodeExecutionError, PluginNodeExecutorLogicPort,
        PluginNodeStateScope as PersistenceStateScope, plugin_invocation_cancellation_target,
        plugin_node_invocation_identity,
    },
    plugin_authorization::{ApprovedPluginTurnAuthorization, ProductionPluginTurnAuthorization},
    plugin_outcome::{
        CanonicalPluginBudgetState, PluginNodeOutcomeValidationError, PluginNodeOutcomeValidator,
        ValidatePluginNodeOutcomeCommand, ValidatedPluginNodeOutcome,
    },
    plugin_state_turn::{
        LoadPriorPluginNodeStateCommand, PluginStateReadTurnCoordinator, PluginStateReadTurnError,
        PluginStateTurnCoordinator, PluginStateTurnError, PreservePluginNodeStateCommand,
        PreservePluginNodeStateResult, PriorPluginNodeState, SessionPluginStateTurnJournal,
    },
    session::{
        CanonicalPluginNodeActionProposal, CanonicalPluginNodeOutcomeProposal,
        PluginNodeInvocationAmbiguousEvent, PluginNodeInvocationAuthorizedEvent,
        PluginNodeInvocationCompletedEvent, PluginNodeInvocationDispatchedEvent,
        PluginNodeInvocationFailedEvent, PluginNodeInvocationIdentity,
        PluginNodeInvocationProposedEvent, PluginNodeInvocationRecord,
        PluginNodeInvocationRecovery, PluginNodeInvocationState, RuntimeCommittedEvent,
        SessionNodeExecutorBoundary, SessionNodeExecutorResolution, SessionNodeExecutorSource,
        SessionReducerError, SessionState, StyleExecutionContract,
        classify_plugin_node_invocation_recovery, derive_plugin_node_state_prior,
        plugin_node_action_hash, plugin_node_actions_hash, plugin_node_value_hash, reduce,
    },
};

const MAX_COORDINATOR_ROUNDS: usize = 16;
const MAX_PLUGIN_NODE_ATTEMPTS: u8 = 10;

/// Exact current canonical head consumed by the plugin-node coordinator.
#[derive(Clone, Debug)]
pub struct PluginTurnHead {
    /// Pure replay projection at the exact journal head.
    pub state: SessionState,
    /// Canonical identity of the latest event.
    pub last_event_id: EventId,
}

/// Runtime-allocated identity material for one canonical lifecycle event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginTurnEventIdentity {
    /// Unique event identity.
    pub event_id: EventId,
    /// Runtime-resolved event time.
    pub timestamp: TimestampMillis,
    /// Session/run correlation identity.
    pub correlation_id: CorrelationId,
}

/// Expected canonical journal position for append compare-and-swap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginTurnAppendPosition {
    /// Current canonical sequence.
    pub sequence: Sequence,
    /// Current canonical event identity.
    pub event_id: EventId,
}

/// Stable plugin-turn journal-boundary failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("plugin turn journal failed: {code}")]
pub struct PluginTurnJournalError {
    /// Bounded stable diagnostic code.
    pub code: String,
}

/// Immutable declaration policy needed for fail-closed receipt recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeInvocationPolicy {
    /// Exact validated declaration hash retained by the persisted plan.
    pub declaration_hash: ContentHash,
    /// Whether repeating the isolated operation is declared idempotent.
    pub idempotent: bool,
    /// Whether the plugin declaration permits external effects.
    pub external_effects: bool,
    /// Maximum isolated worker attempts declared by the plugin.
    pub max_attempts: u8,
    /// Exact canonically ordered permission names from the declaration.
    pub required_permissions: Vec<String>,
}

/// Exact keyed authorization returned before dispatch intent is committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PluginTurnAuthorization {
    /// Digest of the keyed grant/action authorization; the grant stays behind
    /// the authorization boundary.
    pub authorization_digest: ContentHash,
}

/// Complete bounded authorization request for one immutable invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizePluginTurnCommand {
    /// Canonical session.
    pub session_id: SessionId,
    /// Exact immutable invocation identity.
    pub identity: PluginNodeInvocationIdentity,
    /// Exact immutable declaration policy.
    pub policy: PluginNodeInvocationPolicy,
    /// Consequential runtime proposal evaluated by interceptors and both policy
    /// layers before a keyed grant is issued.
    pub proposal: ActionProposal,
    /// Digest of the exact consequential proposal.
    pub action_digest: ContentHash,
    /// Exact persisted source, including the selected plugin identity.
    pub executor_source: SessionNodeExecutorSource,
    /// Exact validated declaration digest.
    pub declaration_hash: ContentHash,
}

/// Stable typed authorization-boundary failure.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum PluginTurnAuthorizationError {
    /// The exact unchanged proposal requires a durable approval continuation.
    #[error("plugin turn authorization requires approval: {reason}")]
    ApprovalRequired {
        /// Exact proposal that may be approved; replacements are never carried here.
        proposal: Box<ActionProposal>,
        /// Bounded safe approval summary.
        reason: String,
        /// Optional interceptor-owned continuation token.
        continuation: Option<String>,
    },
    /// An interceptor attempted to replace the immutable invocation.
    #[error("plugin turn authorization rejected a replacement")]
    ReplacementRejected,
    /// The proposal did not match the immutable invocation contract.
    #[error("plugin turn authorization received an invalid exact proposal")]
    InvalidProposal,
    /// User or mandatory policy denied the proposal.
    #[error("plugin turn authorization denied: {reason}")]
    Denied {
        /// Bounded safe policy reason.
        reason: String,
    },
    /// An interceptor rejected, cancelled, deferred, forked, or aborted evaluation.
    #[error("plugin turn authorization failed closed: {code}")]
    FailedClosed {
        /// Stable bounded classification.
        code: String,
    },
}

/// Logic-owned policy/grant boundary used before durable plugin dispatch.
#[async_trait]
pub trait PluginTurnAuthorizationPort: Send + Sync + 'static {
    /// Authorizes the exact immutable invocation and returns only a grant
    /// digest suitable for canonical persistence.
    ///
    /// # Errors
    ///
    /// Returns a stable failure without crossing the plugin execution boundary.
    async fn authorize_plugin_turn(
        &self,
        command: AuthorizePluginTurnCommand,
    ) -> Result<PluginTurnAuthorization, PluginTurnAuthorizationError>;
}

/// Exact durable terminal receipt body retained outside the canonical journal
/// until it can be reduced.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PluginNodeTerminalReceiptOutcome {
    /// Structurally validated, still non-authoritative node proposal.
    Completed {
        /// Bounded proposal awaiting runtime-owned action/variable validation.
        proposal: Box<CanonicalPluginNodeOutcomeProposal>,
        /// Exact isolated worker attempts.
        attempts: u8,
    },
    /// Definite fail-closed terminal classification.
    Failed {
        /// Stable failure code.
        code: String,
        /// Bounded redacted diagnostic.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diagnostic: Option<String>,
        /// Exact isolated worker attempts.
        attempts: u8,
    },
    /// Effect boundary may have been crossed and must not be redispatched.
    Ambiguous {
        /// Stable ambiguity code.
        code: String,
        /// Bounded redacted diagnostic.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diagnostic: Option<String>,
        /// Exact isolated worker attempts.
        attempts: u8,
    },
}

/// Tamper-evident terminal receipt for one exact plugin-node invocation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginNodeTerminalReceipt {
    /// Complete immutable invocation identity.
    pub identity: PluginNodeInvocationIdentity,
    /// Exact terminal result.
    pub outcome: PluginNodeTerminalReceiptOutcome,
    /// Digest of the identity and terminal result.
    pub receipt_hash: ContentHash,
}

impl PluginNodeTerminalReceipt {
    /// Seals one exact terminal receipt.
    ///
    /// # Errors
    ///
    /// Returns [`PluginTurnError::Serialization`] when receipt material cannot
    /// be serialized.
    pub fn seal(
        identity: PluginNodeInvocationIdentity,
        outcome: PluginNodeTerminalReceiptOutcome,
    ) -> Result<Self, PluginTurnError> {
        let receipt_hash = terminal_receipt_hash(&identity, &outcome)?;
        Ok(Self {
            identity,
            outcome,
            receipt_hash,
        })
    }

    fn validate(&self) -> Result<(), PluginTurnError> {
        if self.receipt_hash != terminal_receipt_hash(&self.identity, &self.outcome)? {
            return Err(PluginTurnError::InvalidReceipt);
        }
        Ok(())
    }
}

/// Narrow storage boundary for canonical journal CAS and durable terminal
/// plugin receipts.
pub trait PluginTurnJournal: Send + Sync + 'static {
    /// Loads and purely replays the exact current session head.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure when load or replay fails.
    fn load(&self) -> Result<PluginTurnHead, PluginTurnJournalError>;

    /// Allocates runtime-owned canonical event identity material.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure when allocation fails.
    fn allocate_identity(&self) -> Result<PluginTurnEventIdentity, PluginTurnJournalError>;

    /// Appends one sealed reducer-validated event at the exact expected head.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure on conflict or durability failure.
    fn append(
        &self,
        expected: PluginTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<(), PluginTurnJournalError>;

    /// Loads an exact durable terminal receipt, if one exists.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure when receipt storage is unavailable.
    fn terminal_receipt(
        &self,
        invocation_id: &str,
    ) -> Result<Option<PluginNodeTerminalReceipt>, PluginTurnJournalError>;

    /// Stores an exact terminal receipt idempotently before its canonical event.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure on conflicting identity or durability
    /// failure.
    fn store_terminal_receipt(
        &self,
        receipt: PluginNodeTerminalReceipt,
    ) -> Result<(), PluginTurnJournalError>;

    /// Reports an exact runtime-owned cancellation request.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary failure when cancellation state is unavailable.
    fn cancellation_requested(&self, cancellation_id: &str)
    -> Result<bool, PluginTurnJournalError>;
}

/// Production journal/receipt/cancellation adapter for one exact session.
///
/// The adapter remains runtime-logic owned and calls only data ports plus the
/// immediately lower persistence logic use case. It never opens files or
/// contacts plugin-host dependencies directly.
#[derive(Clone, Debug)]
pub struct SessionPluginTurnJournal<D> {
    data: D,
    persistence: SessionPersistenceLogic<D>,
    session_id: SessionId,
    session_directory: PathBuf,
}

impl<D> SessionPluginTurnJournal<D>
where
    D: Clone,
{
    /// Binds the production adapter to one canonical session directory.
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

impl<D> PluginTurnJournal for SessionPluginTurnJournal<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + PluginNodeReceiptDataPort
        + RuntimeCancellationDataPort
        + 'static,
{
    fn load(&self) -> Result<PluginTurnHead, PluginTurnJournalError> {
        self.persistence
            .load_session(LoadSessionCommand {
                session_directory: self.session_directory.clone(),
                expected_session_id: self.session_id,
            })
            .map(|loaded| PluginTurnHead {
                state: loaded.state,
                last_event_id: loaded.last_event_id,
            })
            .map_err(|_| plugin_turn_journal_error("load_failed"))
    }

    fn allocate_identity(&self) -> Result<PluginTurnEventIdentity, PluginTurnJournalError> {
        self.data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map(|identity| PluginTurnEventIdentity {
                event_id: identity.event_id,
                timestamp: identity.timestamp,
                correlation_id: identity.correlation_id,
            })
            .map_err(|_| plugin_turn_journal_error("identity_unavailable"))
    }

    fn append(
        &self,
        expected: PluginTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<(), PluginTurnJournalError> {
        if event.metadata.sequence
            != expected
                .sequence
                .checked_next()
                .map_err(|_| plugin_turn_journal_error("sequence_overflow"))?
        {
            return Err(plugin_turn_journal_error("append_sequence_mismatch"));
        }
        let event_id = event.metadata.event_id;
        let sequence = event.metadata.sequence;
        match self
            .persistence
            .compare_append_event(CompareAppendSessionEventCommand {
                session_directory: self.session_directory.clone(),
                expected_head_event_id: expected.event_id,
                event,
                durability: CommitDurability::Data,
            })
            .map_err(|_| plugin_turn_journal_error("append_failed"))?
        {
            CompareAppendSessionEventResult::Conflict => {
                Err(plugin_turn_journal_error("append_conflict"))
            }
            CompareAppendSessionEventResult::Appended(appended)
                if appended.event_id == event_id && appended.sequence == sequence =>
            {
                Ok(())
            }
            CompareAppendSessionEventResult::Appended(_) => {
                Err(plugin_turn_journal_error("append_receipt_mismatch"))
            }
        }
    }

    fn terminal_receipt(
        &self,
        invocation_id: &str,
    ) -> Result<Option<PluginNodeTerminalReceipt>, PluginTurnJournalError> {
        self.data
            .load_plugin_node_receipt(PluginNodeReceiptDataIdentity {
                session_id: self.session_id,
                invocation_id: invocation_id.to_owned(),
            })
            .map_err(map_receipt_load_error)?
            .map(|record| {
                let receipt: PluginNodeTerminalReceipt = serde_json::from_str(&record.receipt_json)
                    .map_err(|_| plugin_turn_journal_error("receipt_corrupt"))?;
                receipt
                    .validate()
                    .map_err(|_| plugin_turn_journal_error("receipt_invalid"))?;
                Ok(receipt)
            })
            .transpose()
    }

    fn store_terminal_receipt(
        &self,
        receipt: PluginNodeTerminalReceipt,
    ) -> Result<(), PluginTurnJournalError> {
        receipt
            .validate()
            .map_err(|_| plugin_turn_journal_error("receipt_invalid"))?;
        if receipt.identity.invocation_id.is_empty() {
            return Err(plugin_turn_journal_error("receipt_identity_invalid"));
        }
        let invocation_id = receipt.identity.invocation_id.clone();
        let receipt_json = serde_json::to_string(&receipt)
            .map_err(|_| plugin_turn_journal_error("receipt_encoding_failed"))?;
        let stored = self
            .data
            .store_plugin_node_receipt(StorePluginNodeReceiptDataRequest {
                identity: PluginNodeReceiptDataIdentity {
                    session_id: self.session_id,
                    invocation_id: invocation_id.clone(),
                },
                receipt_json: receipt_json.clone(),
            })
            .map_err(map_receipt_store_error)?;
        if stored.identity.session_id != self.session_id
            || stored.identity.invocation_id != invocation_id
            || stored.receipt_json != receipt_json
        {
            return Err(plugin_turn_journal_error("receipt_store_mismatch"));
        }
        Ok(())
    }

    fn cancellation_requested(
        &self,
        cancellation_id: &str,
    ) -> Result<bool, PluginTurnJournalError> {
        self.data
            .cancellation_requested(RuntimeCancellationDataRequest {
                cancellation_id: cancellation_id.to_owned(),
            })
            .map_err(|_| plugin_turn_journal_error("cancellation_unavailable"))
    }
}

fn map_receipt_load_error(error: PluginNodeReceiptDataError) -> PluginTurnJournalError {
    plugin_turn_journal_error(match error {
        PluginNodeReceiptDataError::Invalid => "receipt_identity_invalid",
        PluginNodeReceiptDataError::TooLarge => "receipt_too_large",
        PluginNodeReceiptDataError::Unavailable => "receipt_load_unavailable",
        PluginNodeReceiptDataError::Corrupt => "receipt_corrupt",
        PluginNodeReceiptDataError::Conflict => "receipt_conflict",
    })
}

fn map_receipt_store_error(error: PluginNodeReceiptDataError) -> PluginTurnJournalError {
    plugin_turn_journal_error(match error {
        PluginNodeReceiptDataError::Invalid => "receipt_identity_invalid",
        PluginNodeReceiptDataError::TooLarge => "receipt_too_large",
        PluginNodeReceiptDataError::Unavailable => "receipt_store_unavailable",
        PluginNodeReceiptDataError::Corrupt => "receipt_corrupt",
        PluginNodeReceiptDataError::Conflict => "receipt_conflict",
    })
}

/// Production-ready plugin-node runtime seam. Generic turn dispatch is
/// intentionally not wired yet.
#[derive(Clone, Debug)]
pub struct ProductionPluginTurnRuntime<D> {
    data: D,
    session_id: SessionId,
    session_directory: PathBuf,
}

impl<D> ProductionPluginTurnRuntime<D>
where
    D: Clone,
{
    /// Binds the real runtime data composition for one session.
    #[must_use]
    pub fn new(data: D, session_id: SessionId, session_directory: PathBuf) -> Self {
        Self {
            data,
            session_id,
            session_directory,
        }
    }

    /// Creates a live coordinator using the real plugin-host logic port and an
    /// injected runtime authorization gate.
    #[must_use]
    pub fn coordinator<A>(
        &self,
        authorization: Arc<A>,
    ) -> PluginTurnCoordinator<SessionPluginTurnJournal<D>, A, PluginCompositionLogic<D>>
    where
        D: Send
            + Sync
            + EventIdentityDataPort
            + JournalEventDataPort
            + PluginDataPort
            + PluginNodeReceiptDataPort
            + RuntimeCancellationDataPort
            + 'static,
        A: PluginTurnAuthorizationPort,
    {
        PluginTurnCoordinator::new(
            SessionPluginTurnJournal::new(
                self.data.clone(),
                self.session_id,
                self.session_directory.clone(),
            ),
            authorization,
            Arc::new(PluginCompositionLogic::new(self.data.clone())),
        )
    }

    /// Creates a live coordinator bound to the exact executor-declared state
    /// scope.
    #[must_use]
    pub fn coordinator_with_state_scope<A>(
        &self,
        authorization: Arc<A>,
        state_scope: PersistenceStateScope,
    ) -> PluginTurnCoordinator<SessionPluginTurnJournal<D>, A, PluginCompositionLogic<D>>
    where
        D: Send
            + Sync
            + EventIdentityDataPort
            + JournalEventDataPort
            + PluginDataPort
            + PluginNodeReceiptDataPort
            + RuntimeCancellationDataPort
            + 'static,
        A: PluginTurnAuthorizationPort,
    {
        PluginTurnCoordinator::new_with_state_scope(
            SessionPluginTurnJournal::new(
                self.data.clone(),
                self.session_id,
                self.session_directory.clone(),
            ),
            authorization,
            Arc::new(PluginCompositionLogic::new(self.data.clone())),
            state_scope,
        )
    }
}

/// One exact live plugin-node execution request.
#[derive(Clone, Debug, PartialEq)]
pub struct DrivePluginTurnCommand {
    /// Canonical owning session.
    pub session_id: SessionId,
    /// Exact immutable node-work identity.
    pub work: NodeWorkIdentity,
    /// Exact persisted executor resolution.
    pub executor: SessionNodeExecutorResolution,
    /// Bounded typed node input.
    pub input: serde_json::Value,
    /// Explicit bounded readable-state projection.
    pub readable_state: serde_json::Value,
    /// Runtime-owned cancellation identity.
    pub cancellation_id: String,
    /// Exact immutable declaration policy.
    pub policy: PluginNodeInvocationPolicy,
}

/// Terminal result of one plugin-node coordinator drive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PluginTurnOutcome {
    /// The exact unchanged invocation requires a durable user approval before
    /// authorization may be committed.
    AwaitingApproval {
        /// Exact immutable invocation identity.
        identity: Box<PluginNodeInvocationIdentity>,
        /// Digest of the exact consequential action awaiting approval.
        action_digest: ContentHash,
        /// Bounded approval summary.
        reason: String,
        /// Optional opaque interceptor continuation retained for diagnostics.
        interceptor_continuation: Option<String>,
    },
    /// Completion is canonical only as a proposal awaiting normal runtime
    /// action, variable, transition, permission, and budget validation.
    ProposalPendingValidation {
        /// Stable invocation ID.
        invocation_id: String,
        /// Exact bounded proposal.
        proposal: Box<CanonicalPluginNodeOutcomeProposal>,
    },
    /// A definite terminal failure is canonical.
    Failed {
        /// Stable failure code.
        code: String,
    },
    /// Execution is terminally ambiguous and may never be redispatched.
    AmbiguousFailClosed {
        /// Stable ambiguity code.
        code: String,
    },
}

/// Live coordinator result at the exact terminal journal head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginTurnResult {
    /// Canonical sequence after terminal reduction.
    pub last_sequence: Sequence,
    /// Terminal coordinator outcome.
    pub outcome: PluginTurnOutcome,
}

/// Runtime policy mode for one exact live plugin-node drive.
#[derive(Clone)]
#[allow(
    missing_docs,
    reason = "variant fields are the explicit initial-versus-approved authorization contract"
)]
pub enum PluginNodeTurnAuthorizationMode {
    /// Evaluate the ordinary style/plugin/user/mandatory policy stack.
    Initial {
        style_pipeline: Arc<BlockingPipeline<ActionProposal>>,
        plugin_pipeline: Arc<BlockingPipeline<ActionProposal>>,
        capabilities: ActionCapabilities,
        user_policy: PermissionPolicy,
        mandatory_policy: PermissionPolicy,
    },
    /// Resume one exact durable approval and revalidate mandatory policy.
    Approved {
        identity: Box<PluginNodeInvocationIdentity>,
        action_digest: ContentHash,
        mandatory_policy: PermissionPolicy,
    },
}

/// Complete owned input for the optional production plugin-node turn seam.
#[derive(Clone)]
#[allow(
    missing_docs,
    reason = "the owned command fields mirror the documented validation command and durable drive contracts"
)]
pub struct ExecuteLivePluginNodeCommand {
    pub session_id: SessionId,
    pub session_directory: PathBuf,
    pub work: NodeWorkIdentity,
    pub executor: SessionNodeExecutorResolution,
    pub input: serde_json::Value,
    pub readable_state: serde_json::Value,
    pub cancellation_id: String,
    pub authorization: PluginNodeTurnAuthorizationMode,
    pub graph: ExecutableGraph,
    pub execution_contract: StyleExecutionContract,
    pub variables: CanonicalVariableEventReducer,
    pub artifact_store_root: PathBuf,
    pub recorded_runtime_values: BTreeMap<String, CanonicalVariableValue>,
    pub required_preserved_state_keys: BTreeSet<String>,
    pub budget: CanonicalPluginBudgetState,
}

/// Runtime-owned command for applying validated plugin state after canonical
/// outcome, budget, and action application.
#[derive(Clone, Debug)]
pub struct PreserveLivePluginNodeStateCommand {
    /// Canonical owning session.
    pub session_id: SessionId,
    /// Immutable session journal location.
    pub session_directory: PathBuf,
    /// Exact invocation selected by canonical replay.
    pub identity: PluginNodeInvocationIdentity,
    /// Exact persisted executor resolution.
    pub executor: SessionNodeExecutorResolution,
    /// Canonical outcome-validation marker authorizing state application.
    pub validation_hash: ContentHash,
    /// Bounded raw plugin-owned state, never copied into the journal.
    pub state: serde_json::Value,
    /// Exact executor-declaration state scope.
    pub declared_state_scope: String,
    /// Runtime-owned cancellation identity.
    pub cancellation_id: String,
}

/// Exact runtime-owned request to cancel or reconcile one canonical plugin invocation.
#[derive(Clone, Debug)]
#[allow(
    missing_docs,
    reason = "command fields are the exact canonical invocation and authenticated cancellation material"
)]
pub struct CancelLivePluginNodeCommand {
    pub session_id: SessionId,
    pub session_directory: PathBuf,
    pub identity: PluginNodeInvocationIdentity,
    pub input: serde_json::Value,
    pub readable_state: serde_json::Value,
    pub reason_code: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
}

/// Logic-owned copy of the authenticated plugin-host cancellation receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    missing_docs,
    reason = "receipt fields intentionally mirror the complete logic-owned authenticated receipt"
)]
pub struct LivePluginNodeCancellationReceipt {
    pub target: PluginInvocationCancellationTarget,
    pub reason_code: String,
    pub action_digest: ContentHash,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
    pub status: LivePluginNodeHostCancellationStatus,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
}

/// Exact plugin-host cancellation acknowledgement classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    missing_docs,
    reason = "status names are the protocol-independent meanings"
)]
pub enum LivePluginNodeHostCancellationStatus {
    Signalled,
    AlreadyTerminal,
}

/// Canonical cancellation/reconciliation result returned without exposing data types.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    missing_docs,
    reason = "outcome fields retain the exact canonical invocation and optional cancellation receipt"
)]
pub enum LivePluginNodeCancellationOutcome {
    CancelledBeforeDispatch {
        invocation: Box<PluginNodeInvocationRecord>,
    },
    TerminalReceiptReconciled {
        invocation: Box<PluginNodeInvocationRecord>,
        receipt: Box<LivePluginNodeCancellationReceipt>,
    },
    AlreadyTerminal {
        invocation: Box<PluginNodeInvocationRecord>,
    },
    AmbiguousNoProof {
        invocation: Box<PluginNodeInvocationRecord>,
        receipt: Option<Box<LivePluginNodeCancellationReceipt>>,
    },
}

#[derive(Clone, Debug)]
struct CancelExactPluginInvocationCommand {
    target: PluginInvocationCancellationTarget,
    reason_code: String,
    nonce: String,
    idempotency_key: String,
    cancellation_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactPluginCancellationError {
    Invalid,
    Unconfirmed,
}

#[async_trait]
trait ExactPluginCancellationPort: Send + Sync {
    async fn cancel_exact_plugin_invocation(
        &self,
        command: CancelExactPluginInvocationCommand,
    ) -> Result<LivePluginNodeCancellationReceipt, ExactPluginCancellationError>;
}

/// Result returned to generic orchestration without exposing a dependency
/// implementation or allowing the plugin to commit canonical graph state.
#[derive(Clone, Debug, PartialEq)]
#[allow(
    missing_docs,
    reason = "outcome fields are exact pass-through identities described by each variant"
)]
pub enum LivePluginNodeOutcome {
    AwaitingApproval {
        identity: Box<PluginNodeInvocationIdentity>,
        action_digest: ContentHash,
        reason: String,
        interceptor_continuation: Option<String>,
    },
    Validated {
        identity: Box<PluginNodeInvocationIdentity>,
        proposal: Box<CanonicalPluginNodeOutcomeProposal>,
        canonical_invocation: Box<PluginNodeInvocationRecord>,
        validated: Box<ValidatedPluginNodeOutcome>,
    },
    Failed {
        code: String,
        ambiguous: bool,
    },
}

/// Optional logic-owned seam injected into turn orchestration. This preserves
/// existing non-plugin turn data bounds and fails closed when absent.
#[async_trait]
#[allow(
    missing_docs,
    reason = "the single method executes the complete live plugin-node contract described by the trait"
)]
pub trait PluginNodeTurnPort: Send + Sync {
    async fn execute_live_plugin_node(
        &self,
        command: ExecuteLivePluginNodeCommand,
    ) -> Result<LivePluginNodeOutcome, PluginNodeTurnRuntimeError>;

    /// Executes the same exact immutable plugin work inside one replayed
    /// parallel branch. Implementations must retain the branch identity during
    /// output validation; silently falling back to top-level validation would
    /// authorize shared writes without the parallel coordinator.
    async fn execute_live_plugin_node_in_branch(
        &self,
        _command: ExecuteLivePluginNodeCommand,
        _branch: BranchWriteContext,
    ) -> Result<LivePluginNodeOutcome, PluginNodeTurnRuntimeError> {
        Err(PluginNodeTurnRuntimeError::BranchExecutionUnavailable)
    }

    async fn preserve_live_plugin_node_state(
        &self,
        _command: PreserveLivePluginNodeStateCommand,
    ) -> Result<PreservePluginNodeStateResult, PluginNodeTurnRuntimeError> {
        Err(PluginNodeTurnRuntimeError::StatePersistenceUnavailable)
    }

    async fn cancel_live_plugin_node(
        &self,
        _command: CancelLivePluginNodeCommand,
    ) -> Result<LivePluginNodeCancellationOutcome, PluginNodeTurnRuntimeError> {
        Err(PluginNodeTurnRuntimeError::CancellationUnavailable)
    }
}

/// Production adapter over the runtime data/plugin-host/receipt/cancellation
/// boundaries.
#[derive(Clone)]
pub struct ProductionPluginNodeTurnPort<D> {
    data: D,
}

impl<D> ProductionPluginNodeTurnPort<D> {
    /// Creates an injectable production adapter over the composed runtime data
    /// boundary.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

#[async_trait]
impl<D> ExactPluginCancellationPort for ProductionPluginNodeTurnPort<D>
where
    D: PluginDataPort + Send + Sync,
{
    async fn cancel_exact_plugin_invocation(
        &self,
        command: CancelExactPluginInvocationCommand,
    ) -> Result<LivePluginNodeCancellationReceipt, ExactPluginCancellationError> {
        self.data
            .cancel_node_invocation(CancelPluginNodeInvocationDataRequest {
                target: crate::plugin::map_cancellation_target(&command.target),
                reason_code: command.reason_code,
                nonce: command.nonce,
                idempotency_key: command.idempotency_key,
                cancellation_id: command.cancellation_id,
            })
            .await
            .map(map_live_cancellation_receipt)
            .map_err(|error| match error {
                PluginDataError::Invalid => ExactPluginCancellationError::Invalid,
                _ => ExactPluginCancellationError::Unconfirmed,
            })
    }
}

#[async_trait]
impl<D> PluginNodeTurnPort for ProductionPluginNodeTurnPort<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + PluginDataPort
        + PluginNodeReceiptDataPort
        + RuntimeCancellationDataPort
        + ArtifactDataPort
        + 'static,
{
    async fn cancel_live_plugin_node(
        &self,
        command: CancelLivePluginNodeCommand,
    ) -> Result<LivePluginNodeCancellationOutcome, PluginNodeTurnRuntimeError> {
        let SessionNodeExecutorSource::Plugin { plugin_id } = &command.identity.executor.source
        else {
            return Err(PluginNodeTurnRuntimeError::InvalidCancellation);
        };
        let declaration = self
            .data
            .node_executor_declaration(
                plugin_id,
                &command.identity.executor.executor_id,
                &command.identity.executor.executor_version,
                &command.identity.executor.node_kind,
            )
            .map_err(|_| PluginNodeTurnRuntimeError::DeclarationUnavailable)?;
        let configuration_reference = self
            .data
            .plugin_configuration_reference(plugin_id)
            .map_err(|_| PluginNodeTurnRuntimeError::DeclarationUnavailable)?;
        if command.identity.executor.boundary != SessionNodeExecutorBoundary::PluginHost
            || command.identity.plugin_id != *plugin_id
            || command.identity.work.run_id.is_empty()
            || command.identity.executor.executor_declaration_hash != declaration.declaration_hash
            || command.identity.configuration_hash
                != command.identity.executor.adapter_configuration_reference
            || plugin_node_value_hash(&command.input)
                .map_err(|_| PluginNodeTurnRuntimeError::InvalidCancellation)?
                != command.identity.input_hash
            || plugin_node_value_hash(&command.readable_state)
                .map_err(|_| PluginNodeTurnRuntimeError::InvalidCancellation)?
                != command.identity.readable_state_hash
            || command.reason_code.is_empty()
            || command.nonce.is_empty()
            || command.idempotency_key.is_empty()
            || command.cancellation_id.is_empty()
        {
            return Err(PluginNodeTurnRuntimeError::InvalidCancellation);
        }
        let request_hash = serde_json::to_vec(&(
            "agentmod.plugin.node-executor.request.v1",
            plugin_id,
            &command.identity.invocation_id,
            &command.identity.executor.executor_id,
            &command.identity.executor.executor_version,
            &command.identity.executor.node_kind,
            &declaration.handler,
            declaration.timeout_ms,
            configuration_reference,
            &command.input,
            &command.readable_state,
        ))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginNodeTurnRuntimeError::InvalidCancellation)?;
        let target = plugin_invocation_cancellation_target(
            &command.session_id.to_string(),
            &command.identity.work.run_id,
            plugin_id,
            &declaration.plugin_version,
            &command.identity.invocation_id,
            &command.identity.executor.executor_id,
            declaration.declaration_hash,
            request_hash,
        )
        .map_err(|_| PluginNodeTurnRuntimeError::InvalidCancellation)?;
        let journal = SessionPluginTurnJournal::new(
            self.data.clone(),
            command.session_id,
            command.session_directory,
        );
        cancel_and_reconcile_plugin_invocation(
            &journal,
            self,
            command.session_id,
            command.identity,
            CancelExactPluginInvocationCommand {
                target,
                reason_code: command.reason_code,
                nonce: command.nonce,
                idempotency_key: command.idempotency_key,
                cancellation_id: command.cancellation_id,
            },
        )
        .await
    }

    async fn preserve_live_plugin_node_state(
        &self,
        command: PreserveLivePluginNodeStateCommand,
    ) -> Result<PreservePluginNodeStateResult, PluginNodeTurnRuntimeError> {
        let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
            return Err(PluginNodeTurnRuntimeError::InvalidResolution);
        };
        if command.executor != command.identity.executor
            || command.identity.plugin_id != *plugin_id
            || command.executor.boundary != SessionNodeExecutorBoundary::PluginHost
        {
            return Err(PluginNodeTurnRuntimeError::InvalidResolution);
        }
        let declaration = self
            .data
            .node_executor_declaration(
                plugin_id,
                &command.executor.executor_id,
                &command.executor.executor_version,
                &command.executor.node_kind,
            )
            .map_err(|_| PluginNodeTurnRuntimeError::DeclarationUnavailable)?;
        if declaration.declaration_hash != command.executor.executor_declaration_hash
            || declaration.state_scope != command.declared_state_scope
        {
            return Err(PluginNodeTurnRuntimeError::InvalidResolution);
        }
        let plugin_configuration_reference = self
            .data
            .plugin_configuration_reference(plugin_id)
            .map_err(|_| PluginNodeTurnRuntimeError::DeclarationUnavailable)?;
        let state_scope = parse_state_scope(&declaration.state_scope)?;
        PluginStateTurnCoordinator::new(
            SessionPluginStateTurnJournal::new(
                self.data.clone(),
                command.session_id,
                command.session_directory,
            ),
            PluginCompositionLogic::new(self.data.clone()),
        )
        .preserve(PreservePluginNodeStateCommand {
            session_id: command.session_id,
            identity: command.identity,
            plugin_version: declaration.plugin_version,
            plugin_configuration_reference,
            validation_hash: command.validation_hash,
            state: command.state,
            state_scope,
            cancellation_id: command.cancellation_id,
        })
        .await
        .map_err(PluginNodeTurnRuntimeError::StatePersistence)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the production seam keeps declaration lookup, durable drive, replay reload, and runtime outcome validation at one auditable boundary"
    )]
    async fn execute_live_plugin_node(
        &self,
        command: ExecuteLivePluginNodeCommand,
    ) -> Result<LivePluginNodeOutcome, PluginNodeTurnRuntimeError> {
        self.execute_live_plugin_node_scoped(command, None).await
    }

    async fn execute_live_plugin_node_in_branch(
        &self,
        command: ExecuteLivePluginNodeCommand,
        branch: BranchWriteContext,
    ) -> Result<LivePluginNodeOutcome, PluginNodeTurnRuntimeError> {
        self.execute_live_plugin_node_scoped(command, Some(branch))
            .await
    }
}

impl<D> ProductionPluginNodeTurnPort<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + PluginDataPort
        + PluginNodeReceiptDataPort
        + RuntimeCancellationDataPort
        + ArtifactDataPort
        + 'static,
{
    #[allow(
        clippy::too_many_lines,
        reason = "the production seam keeps declaration lookup, durable drive, replay reload, branch binding, and outcome validation at one auditable boundary"
    )]
    async fn execute_live_plugin_node_scoped(
        &self,
        command: ExecuteLivePluginNodeCommand,
        branch: Option<BranchWriteContext>,
    ) -> Result<LivePluginNodeOutcome, PluginNodeTurnRuntimeError> {
        validate_live_plugin_branch_context(&command.work, branch.as_ref())?;
        let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
            return Err(PluginNodeTurnRuntimeError::InvalidResolution);
        };
        if command.executor.boundary != SessionNodeExecutorBoundary::PluginHost
            || command.executor.node_id != command.work.node_id
        {
            return Err(PluginNodeTurnRuntimeError::InvalidResolution);
        }
        let declaration = self
            .data
            .node_executor_declaration(
                plugin_id,
                &command.executor.executor_id,
                &command.executor.executor_version,
                &command.executor.node_kind,
            )
            .map_err(|_| PluginNodeTurnRuntimeError::DeclarationUnavailable)?;
        if declaration.declaration_hash != command.executor.executor_declaration_hash {
            return Err(PluginNodeTurnRuntimeError::InvalidResolution);
        }
        let plugin_configuration_reference = self
            .data
            .plugin_configuration_reference(plugin_id)
            .map_err(|_| PluginNodeTurnRuntimeError::DeclarationUnavailable)?;
        let state_scope = parse_state_scope(&declaration.state_scope)?;
        let mut readable_state = command.readable_state.clone();
        validate_readable_state_projection(&readable_state)?;
        let state_head = SessionPersistenceLogic::new(self.data.clone())
            .load_session(LoadSessionCommand {
                session_directory: command.session_directory.clone(),
                expected_session_id: command.session_id,
            })
            .map_err(|_| PluginNodeTurnRuntimeError::Projection)?;
        let existing = invocation_for_work(&state_head.state, &command.work, &command.executor)?;
        let skip_live_read = existing.as_ref().is_some_and(|record| {
            recovery_uses_canonical_readable_identity(classify_plugin_node_invocation_recovery(
                &state_head.state,
                &record.identity.invocation_id,
            ))
        });
        if !skip_live_read {
            let read_identity = state_read_seed_identity(
                &command,
                plugin_id,
                state_head.last_event_id,
                &readable_state,
            )?;
            let prior = PluginStateReadTurnCoordinator::new(
                SessionPluginStateTurnJournal::new(
                    self.data.clone(),
                    command.session_id,
                    command.session_directory.clone(),
                ),
                PluginCompositionLogic::new(self.data.clone()),
            )
            .load(LoadPriorPluginNodeStateCommand {
                session_id: command.session_id,
                identity: read_identity,
                plugin_version: declaration.plugin_version.clone(),
                plugin_configuration_reference,
                state_scope,
                cancellation_id: command.cancellation_id.clone(),
            })
            .await
            .map_err(PluginNodeTurnRuntimeError::StateRead)?;
            readable_state = merge_prior_plugin_state(readable_state, prior)?;
        }
        let required_permissions = declaration
            .network_permissions
            .iter()
            .map(|permission| format!("network.{permission}"))
            .chain(
                declaration
                    .tool_permissions
                    .iter()
                    .map(|permission| format!("tool.{permission}")),
            )
            .collect();
        let drive = DrivePluginTurnCommand {
            session_id: command.session_id,
            work: command.work.clone(),
            executor: command.executor.clone(),
            input: command.input,
            readable_state,
            cancellation_id: command.cancellation_id,
            policy: PluginNodeInvocationPolicy {
                declaration_hash: declaration.declaration_hash,
                idempotent: declaration.idempotent,
                external_effects: declaration.external_effects,
                max_attempts: declaration.max_attempts,
                required_permissions,
            },
        };
        let runtime = ProductionPluginTurnRuntime::new(
            self.data.clone(),
            command.session_id,
            command.session_directory.clone(),
        );
        let result = match command.authorization {
            PluginNodeTurnAuthorizationMode::Initial {
                style_pipeline,
                plugin_pipeline,
                capabilities,
                user_policy,
                mandatory_policy,
            } => {
                runtime
                    .coordinator_with_state_scope(
                        Arc::new(ProductionPluginTurnAuthorization::new(
                            style_pipeline,
                            plugin_pipeline,
                            capabilities,
                            user_policy,
                            mandatory_policy,
                        )),
                        state_scope,
                    )
                    .drive(drive)
                    .await?
            }
            PluginNodeTurnAuthorizationMode::Approved {
                identity,
                action_digest,
                mandatory_policy,
            } => {
                runtime
                    .coordinator_with_state_scope(
                        Arc::new(ApprovedPluginTurnAuthorization::new(
                            *identity,
                            action_digest,
                            mandatory_policy,
                        )),
                        state_scope,
                    )
                    .drive(drive)
                    .await?
            }
        };
        match result.outcome {
            PluginTurnOutcome::AwaitingApproval {
                identity,
                action_digest,
                reason,
                interceptor_continuation,
            } => Ok(LivePluginNodeOutcome::AwaitingApproval {
                identity,
                action_digest,
                reason,
                interceptor_continuation,
            }),
            PluginTurnOutcome::Failed { code } => Ok(LivePluginNodeOutcome::Failed {
                code,
                ambiguous: false,
            }),
            PluginTurnOutcome::AmbiguousFailClosed { code } => Ok(LivePluginNodeOutcome::Failed {
                code,
                ambiguous: true,
            }),
            PluginTurnOutcome::ProposalPendingValidation {
                invocation_id,
                proposal,
            } => {
                let loaded = SessionPersistenceLogic::new(self.data.clone())
                    .load_session(LoadSessionCommand {
                        session_directory: command.session_directory,
                        expected_session_id: command.session_id,
                    })
                    .map_err(|_| PluginNodeTurnRuntimeError::Projection)?;
                let canonical_invocation = loaded
                    .state
                    .style_execution
                    .as_ref()
                    .and_then(|execution| execution.plugin_node_invocations.get(&invocation_id))
                    .cloned()
                    .ok_or(PluginNodeTurnRuntimeError::Projection)?;
                let identity = canonical_invocation.identity.clone();
                let validated = PluginNodeOutcomeValidator::new(self.data.clone())
                    .validate(ValidatePluginNodeOutcomeCommand {
                        session_id: command.session_id,
                        work: &command.work,
                        graph: &command.graph,
                        execution_contract: &command.execution_contract,
                        executor: &command.executor,
                        declaration: &declaration,
                        variables: &command.variables,
                        proposal: &proposal,
                        receipt_identity: &identity,
                        canonical_invocation: &canonical_invocation,
                        artifact_store_root: command.artifact_store_root,
                        branch,
                        recorded_runtime_values: command.recorded_runtime_values,
                        required_preserved_state_keys: command.required_preserved_state_keys,
                        budget: command.budget,
                    })
                    .map_err(PluginNodeTurnRuntimeError::OutcomeValidation)?;
                Ok(LivePluginNodeOutcome::Validated {
                    identity: Box::new(identity),
                    proposal,
                    canonical_invocation: Box::new(canonical_invocation),
                    validated: Box::new(validated),
                })
            }
        }
    }
}

async fn cancel_and_reconcile_plugin_invocation<J, C>(
    journal: &J,
    cancellation: &C,
    session_id: SessionId,
    identity: PluginNodeInvocationIdentity,
    command: CancelExactPluginInvocationCommand,
) -> Result<LivePluginNodeCancellationOutcome, PluginNodeTurnRuntimeError>
where
    J: PluginTurnJournal,
    C: ExactPluginCancellationPort,
{
    let head = journal.load().map_err(PluginTurnError::from)?;
    let record = exact_cancellation_record(&head, session_id, &identity)?;
    match record.state {
        PluginNodeInvocationState::Completed
        | PluginNodeInvocationState::Failed
        | PluginNodeInvocationState::Ambiguous => {
            return Ok(LivePluginNodeCancellationOutcome::AlreadyTerminal {
                invocation: Box::new(record),
            });
        }
        PluginNodeInvocationState::Proposed | PluginNodeInvocationState::Authorized => {
            let committed = commit_plugin_event(
                journal,
                head,
                RuntimeCommittedEvent::PluginNodeInvocationFailed(Box::new(
                    PluginNodeInvocationFailedEvent {
                        identity: identity.clone(),
                        prior_event_id: record.latest_event_id,
                        code: String::from("cancelled_before_plugin_dispatch"),
                        diagnostic: None,
                        attempts: 0,
                    },
                )),
            )?;
            return Ok(LivePluginNodeCancellationOutcome::CancelledBeforeDispatch {
                invocation: Box::new(exact_cancellation_record(
                    &committed, session_id, &identity,
                )?),
            });
        }
        PluginNodeInvocationState::Dispatched => {}
    }

    let cancellation_receipt = match cancellation.cancel_exact_plugin_invocation(command).await {
        Ok(receipt) => Some(receipt),
        Err(ExactPluginCancellationError::Invalid) => {
            return Err(PluginNodeTurnRuntimeError::InvalidCancellation);
        }
        Err(ExactPluginCancellationError::Unconfirmed) => None,
    };
    let refreshed = journal.load().map_err(PluginTurnError::from)?;
    let refreshed_record = exact_cancellation_record(&refreshed, session_id, &identity)?;
    if refreshed_record.state != PluginNodeInvocationState::Dispatched {
        return Ok(LivePluginNodeCancellationOutcome::AlreadyTerminal {
            invocation: Box::new(refreshed_record),
        });
    }
    if let Some(terminal) = journal
        .terminal_receipt(&identity.invocation_id)
        .map_err(PluginTurnError::from)?
    {
        let committed = commit_plugin_terminal_receipt(journal, refreshed, &identity, terminal)?;
        let invocation = exact_cancellation_record(&committed, session_id, &identity)?;
        return match cancellation_receipt {
            Some(receipt) => Ok(
                LivePluginNodeCancellationOutcome::TerminalReceiptReconciled {
                    invocation: Box::new(invocation),
                    receipt: Box::new(receipt),
                },
            ),
            None => Ok(LivePluginNodeCancellationOutcome::AlreadyTerminal {
                invocation: Box::new(invocation),
            }),
        };
    }
    let committed = commit_plugin_event(
        journal,
        refreshed,
        RuntimeCommittedEvent::PluginNodeInvocationAmbiguous(Box::new(
            PluginNodeInvocationAmbiguousEvent {
                identity: identity.clone(),
                prior_event_id: refreshed_record.latest_event_id,
                code: String::from("plugin_cancellation_terminal_receipt_missing"),
                diagnostic: None,
                attempts: 1,
            },
        )),
    )?;
    Ok(LivePluginNodeCancellationOutcome::AmbiguousNoProof {
        invocation: Box::new(exact_cancellation_record(
            &committed, session_id, &identity,
        )?),
        receipt: cancellation_receipt.map(Box::new),
    })
}

fn exact_cancellation_record(
    head: &PluginTurnHead,
    session_id: SessionId,
    identity: &PluginNodeInvocationIdentity,
) -> Result<PluginNodeInvocationRecord, PluginNodeTurnRuntimeError> {
    if head.state.id != session_id {
        return Err(PluginNodeTurnRuntimeError::InvalidCancellation);
    }
    let record = head
        .state
        .style_execution
        .as_ref()
        .and_then(|execution| {
            execution
                .plugin_node_invocations
                .get(&identity.invocation_id)
        })
        .filter(|record| record.identity == *identity)
        .cloned()
        .ok_or(PluginNodeTurnRuntimeError::InvalidCancellation)?;
    Ok(record)
}

fn map_live_cancellation_receipt(
    receipt: PluginInvocationCancellationDataResult,
) -> LivePluginNodeCancellationReceipt {
    LivePluginNodeCancellationReceipt {
        target: PluginInvocationCancellationTarget {
            session_id: receipt.target.session_id,
            run_id: receipt.target.run_id,
            plugin_id: receipt.target.plugin_id,
            plugin_version: receipt.target.plugin_version,
            invocation_id: receipt.target.invocation_id,
            invocation_digest: receipt.target.invocation_digest,
            operation_id: receipt.target.operation_id,
            declaration_hash: receipt.target.declaration_hash,
            request_hash: receipt.target.request_hash,
        },
        reason_code: receipt.reason_code,
        action_digest: receipt.action_digest,
        nonce: receipt.nonce,
        idempotency_key: receipt.idempotency_key,
        cancellation_id: receipt.cancellation_id,
        status: match receipt.status {
            PluginInvocationCancellationDataStatus::Signalled => {
                LivePluginNodeHostCancellationStatus::Signalled
            }
            PluginInvocationCancellationDataStatus::AlreadyTerminal => {
                LivePluginNodeHostCancellationStatus::AlreadyTerminal
            }
        },
        receipt_id: receipt.receipt_id,
        receipt_digest: receipt.receipt_digest,
    }
}

fn commit_plugin_terminal_receipt<J: PluginTurnJournal>(
    journal: &J,
    head: PluginTurnHead,
    identity: &PluginNodeInvocationIdentity,
    receipt: PluginNodeTerminalReceipt,
) -> Result<PluginTurnHead, PluginTurnError> {
    receipt.validate()?;
    if receipt.identity != *identity {
        return Err(PluginTurnError::InvalidReceipt);
    }
    let prior_event_id = invocation_record(&head.state, identity)?.latest_event_id;
    let payload = match receipt.outcome {
        PluginNodeTerminalReceiptOutcome::Completed { proposal, attempts } => {
            RuntimeCommittedEvent::PluginNodeInvocationCompleted(Box::new(
                PluginNodeInvocationCompletedEvent {
                    identity: identity.clone(),
                    prior_event_id,
                    proposal: *proposal,
                    attempts,
                },
            ))
        }
        PluginNodeTerminalReceiptOutcome::Failed {
            code,
            diagnostic,
            attempts,
        } => RuntimeCommittedEvent::PluginNodeInvocationFailed(Box::new(
            PluginNodeInvocationFailedEvent {
                identity: identity.clone(),
                prior_event_id,
                code,
                diagnostic,
                attempts,
            },
        )),
        PluginNodeTerminalReceiptOutcome::Ambiguous {
            code,
            diagnostic,
            attempts,
        } => RuntimeCommittedEvent::PluginNodeInvocationAmbiguous(Box::new(
            PluginNodeInvocationAmbiguousEvent {
                identity: identity.clone(),
                prior_event_id,
                code,
                diagnostic,
                attempts,
            },
        )),
    };
    commit_plugin_event(journal, head, payload)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "consuming the reducer-validated head prevents accidental reuse after append"
)]
fn commit_plugin_event<J: PluginTurnJournal>(
    journal: &J,
    mut head: PluginTurnHead,
    payload: RuntimeCommittedEvent,
) -> Result<PluginTurnHead, PluginTurnError> {
    const MAX_CONCURRENT_APPEND_RECONCILIATIONS: u8 = 8;
    for reconciliation in 0..=MAX_CONCURRENT_APPEND_RECONCILIATIONS {
        let identity = journal.allocate_identity()?;
        let sequence = head
            .state
            .last_sequence
            .checked_next()
            .map_err(|_| PluginTurnError::Sequence)?;
        let event = seal_event(&head, sequence, identity, payload.clone())?;
        let next_state = reduce(Some(head.state.clone()), &event)?;
        match journal.append(
            PluginTurnAppendPosition {
                sequence: head.state.last_sequence,
                event_id: head.last_event_id,
            },
            event,
        ) {
            Ok(()) => {
                return Ok(PluginTurnHead {
                    state: next_state,
                    last_event_id: identity.event_id,
                });
            }
            Err(error)
                if error.code == "append_conflict"
                    && reconciliation < MAX_CONCURRENT_APPEND_RECONCILIATIONS =>
            {
                let refreshed = journal.load()?;
                if refreshed.state.last_sequence == head.state.last_sequence
                    && refreshed.last_event_id == head.last_event_id
                {
                    return Err(error.into());
                }
                head = refreshed;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(plugin_turn_journal_error("append_conflict").into())
}

fn parse_state_scope(scope: &str) -> Result<PersistenceStateScope, PluginNodeTurnRuntimeError> {
    match scope {
        "invocation" => Ok(PersistenceStateScope::Invocation),
        "session" => Ok(PersistenceStateScope::Session),
        "model_call" | "turn" | "project" | "user" | "runtime" => {
            Err(PluginNodeTurnRuntimeError::UnsupportedStateScope)
        }
        _ => Err(PluginNodeTurnRuntimeError::InvalidResolution),
    }
}

fn validate_readable_state_projection(
    readable_state: &serde_json::Value,
) -> Result<(), PluginNodeTurnRuntimeError> {
    let object = readable_state
        .as_object()
        .ok_or(PluginNodeTurnRuntimeError::InvalidReadableState)?;
    if object.contains_key("$plugin_state") {
        return Err(PluginNodeTurnRuntimeError::PluginStateCollision);
    }
    Ok(())
}

fn validate_live_plugin_branch_context(
    work: &NodeWorkIdentity,
    branch: Option<&BranchWriteContext>,
) -> Result<(), PluginNodeTurnRuntimeError> {
    match (work.branch_path.last(), branch) {
        (None, None) => Ok(()),
        (Some(expected), Some(branch))
            if !branch.branch_id.is_empty()
                && branch.branch_id == *expected
                && !branch.serialized_shared_write =>
        {
            Ok(())
        }
        _ => Err(PluginNodeTurnRuntimeError::InvalidBranchContext),
    }
}

fn merge_prior_plugin_state(
    mut readable_state: serde_json::Value,
    prior: PriorPluginNodeState,
) -> Result<serde_json::Value, PluginNodeTurnRuntimeError> {
    let object = readable_state
        .as_object_mut()
        .ok_or(PluginNodeTurnRuntimeError::InvalidReadableState)?;
    if object.contains_key("$plugin_state") {
        return Err(PluginNodeTurnRuntimeError::PluginStateCollision);
    }
    if let PriorPluginNodeState::Loaded { state, .. } = prior {
        object.insert(String::from("$plugin_state"), state);
    }
    Ok(readable_state)
}

fn state_read_seed_identity(
    command: &ExecuteLivePluginNodeCommand,
    plugin_id: &str,
    causation_event_id: EventId,
    readable_state: &serde_json::Value,
) -> Result<PluginNodeInvocationIdentity, PluginNodeTurnRuntimeError> {
    Ok(PluginNodeInvocationIdentity {
        work: command.work.clone(),
        executor: command.executor.clone(),
        configuration_hash: command.executor.adapter_configuration_reference,
        plugin_id: plugin_id.to_owned(),
        invocation_id: String::from("plugin-state-read-seed"),
        invocation_digest: ContentHash::digest(b"plugin-state-read-seed"),
        input_hash: plugin_node_value_hash(&command.input)
            .map_err(|_| PluginNodeTurnRuntimeError::InvalidReadableState)?,
        readable_state_hash: plugin_node_value_hash(readable_state)
            .map_err(|_| PluginNodeTurnRuntimeError::InvalidReadableState)?,
        causation_event_id,
    })
}

fn invocation_for_work(
    state: &SessionState,
    work: &NodeWorkIdentity,
    executor: &SessionNodeExecutorResolution,
) -> Result<Option<PluginNodeInvocationRecord>, PluginNodeTurnRuntimeError> {
    let Some(execution) = state.style_execution.as_ref() else {
        return Err(PluginNodeTurnRuntimeError::Projection);
    };
    let mut matching = execution
        .plugin_node_invocations
        .values()
        .filter(|record| record.identity.work == *work && record.identity.executor == *executor);
    let result = matching.next().cloned();
    if matching.next().is_some() {
        return Err(PluginNodeTurnRuntimeError::Projection);
    }
    Ok(result)
}

const fn recovery_uses_canonical_readable_identity(recovery: PluginNodeInvocationRecovery) -> bool {
    matches!(
        recovery,
        PluginNodeInvocationRecovery::WaitingForTerminalReceipt
            | PluginNodeInvocationRecovery::CompleteFromCanonicalProposal
            | PluginNodeInvocationRecovery::TerminallyFailed
            | PluginNodeInvocationRecovery::AmbiguousFailClosed
    )
}

/// Stable failure at the optional plugin-node runtime seam.
#[derive(Debug, Error)]
#[allow(
    missing_docs,
    reason = "stable variant messages are the public redacted runtime diagnostics"
)]
pub enum PluginNodeTurnRuntimeError {
    #[error("plugin node resolution is invalid")]
    InvalidResolution,
    #[error("the exact plugin node declaration is unavailable")]
    DeclarationUnavailable,
    #[error("plugin node replay projection is invalid")]
    Projection,
    #[error("plugin node readable state must be an object")]
    InvalidReadableState,
    #[error("plugin node readable state collides with reserved `$plugin_state`")]
    PluginStateCollision,
    #[error("plugin node state scope is unsupported")]
    UnsupportedStateScope,
    #[error("plugin node prior-state read failed: {0}")]
    StateRead(PluginStateReadTurnError),
    #[error("plugin node state persistence boundary is unavailable")]
    StatePersistenceUnavailable,
    #[error("plugin node branch execution boundary is unavailable")]
    BranchExecutionUnavailable,
    #[error("plugin node branch identity does not match immutable work")]
    InvalidBranchContext,
    #[error("plugin node cancellation boundary is unavailable")]
    CancellationUnavailable,
    #[error("plugin node cancellation request does not match the canonical invocation")]
    InvalidCancellation,
    #[error("plugin node state persistence failed: {0}")]
    StatePersistence(PluginStateTurnError),
    #[error("plugin node outcome validation failed: {0}")]
    OutcomeValidation(PluginNodeOutcomeValidationError),
    #[error("plugin node coordination failed: {0}")]
    Turn(#[from] PluginTurnError),
}

/// Durable plugin-node coordinator over injected journal, authorization, and
/// isolated executor boundaries.
pub struct PluginTurnCoordinator<J, A, P> {
    journal: J,
    authorization: Arc<A>,
    executor: Arc<P>,
    state_scope: PersistenceStateScope,
}

impl<J, A, P> PluginTurnCoordinator<J, A, P> {
    /// Creates a coordinator from layer-owned ports.
    #[must_use]
    pub fn new(journal: J, authorization: Arc<A>, executor: Arc<P>) -> Self {
        Self {
            journal,
            authorization,
            executor,
            state_scope: PersistenceStateScope::Invocation,
        }
    }

    /// Creates a coordinator bound to the executor declaration's exact state
    /// scope. Production plugin turns must use this constructor.
    #[must_use]
    pub fn new_with_state_scope(
        journal: J,
        authorization: Arc<A>,
        executor: Arc<P>,
        state_scope: PersistenceStateScope,
    ) -> Self {
        Self {
            journal,
            authorization,
            executor,
            state_scope,
        }
    }
}

impl<J, A, P> PluginTurnCoordinator<J, A, P>
where
    J: PluginTurnJournal,
    A: PluginTurnAuthorizationPort,
    P: PluginNodeExecutorLogicPort + 'static,
{
    /// Drives one exact plugin invocation through the durable outbox.
    ///
    /// # Errors
    ///
    /// Fails closed on projection drift, invalid policy or outcomes, journal
    /// conflicts, receipt tampering, authorization failure persistence, or
    /// bounded coordination exhaustion.
    #[allow(
        clippy::too_many_lines,
        reason = "the complete replay recovery state machine remains adjacent so durable dispatch and no-redispatch behavior can be audited together"
    )]
    pub async fn drive(
        &self,
        command: DrivePluginTurnCommand,
    ) -> Result<PluginTurnResult, PluginTurnError> {
        let identity = self.validate_and_build_identity(&command)?;
        let mut dispatch_committed_here = false;
        for _ in 0..MAX_COORDINATOR_ROUNDS {
            let head = self.journal.load()?;
            self.validate_head(&head, &command, &identity)?;
            match classify_plugin_node_invocation_recovery(&head.state, &identity.invocation_id) {
                PluginNodeInvocationRecovery::NotStarted => {
                    self.commit(
                        head,
                        RuntimeCommittedEvent::PluginNodeInvocationProposed(Box::new(
                            PluginNodeInvocationProposedEvent {
                                identity: identity.clone(),
                            },
                        )),
                    )?;
                }
                PluginNodeInvocationRecovery::AwaitingAuthorization => {
                    let prior_event_id = invocation_record(&head.state, &identity)?.latest_event_id;
                    if self
                        .journal
                        .cancellation_requested(&command.cancellation_id)?
                    {
                        self.commit_failed(
                            head,
                            &identity,
                            prior_event_id,
                            "cancelled_before_authorization",
                            0,
                        )?;
                        continue;
                    }
                    let authorization_command =
                        authorization_command(&head.state, &command, &identity)?;
                    let authorization = self
                        .authorization
                        .authorize_plugin_turn(authorization_command.clone())
                        .await;
                    match authorization {
                        Ok(authorization)
                            if authorization.authorization_digest
                                != ContentHash::from_bytes([0; 32]) =>
                        {
                            self.commit(
                                head,
                                RuntimeCommittedEvent::PluginNodeInvocationAuthorized(Box::new(
                                    PluginNodeInvocationAuthorizedEvent {
                                        identity: identity.clone(),
                                        prior_event_id,
                                        authorization_digest: authorization.authorization_digest,
                                    },
                                )),
                            )?;
                        }
                        Err(PluginTurnAuthorizationError::ApprovalRequired {
                            proposal,
                            reason,
                            continuation,
                        }) => {
                            if proposal.digest().ok() != Some(authorization_command.action_digest)
                                || *proposal != authorization_command.proposal
                            {
                                self.commit_failed(
                                    head,
                                    &identity,
                                    prior_event_id,
                                    "authorization_replacement_rejected",
                                    0,
                                )?;
                                continue;
                            }
                            return Ok(PluginTurnResult {
                                last_sequence: head.state.last_sequence,
                                outcome: PluginTurnOutcome::AwaitingApproval {
                                    identity: Box::new(identity),
                                    action_digest: authorization_command.action_digest,
                                    reason,
                                    interceptor_continuation: continuation,
                                },
                            });
                        }
                        Ok(_) | Err(_) => {
                            self.commit_failed(
                                head,
                                &identity,
                                prior_event_id,
                                "authorization_rejected",
                                0,
                            )?;
                        }
                    }
                }
                PluginNodeInvocationRecovery::SafeToDispatchOnce => {
                    let (prior_event_id, authorization_digest) = {
                        let record = invocation_record(&head.state, &identity)?;
                        (
                            record.latest_event_id,
                            record
                                .authorization_digest
                                .ok_or(PluginTurnError::Projection)?,
                        )
                    };
                    if self
                        .journal
                        .cancellation_requested(&command.cancellation_id)?
                    {
                        self.commit_failed(
                            head,
                            &identity,
                            prior_event_id,
                            "cancelled_before_dispatch",
                            0,
                        )?;
                        continue;
                    }
                    let dispatch_digest = dispatch_digest(&identity, authorization_digest)?;
                    self.commit(
                        head,
                        RuntimeCommittedEvent::PluginNodeInvocationDispatched(Box::new(
                            PluginNodeInvocationDispatchedEvent {
                                identity: identity.clone(),
                                prior_event_id,
                                authorization_digest,
                                dispatch_digest,
                            },
                        )),
                    )?;
                    dispatch_committed_here = true;
                }
                PluginNodeInvocationRecovery::WaitingForTerminalReceipt => {
                    let prior_event_id = invocation_record(&head.state, &identity)?.latest_event_id;
                    if let Some(receipt) = self.journal.terminal_receipt(&identity.invocation_id)? {
                        self.commit_receipt(head, prior_event_id, &identity, receipt)?;
                        continue;
                    }
                    if dispatch_committed_here {
                        dispatch_committed_here = false;
                        self.invoke_once_and_store(&command, &identity).await?;
                        continue;
                    }
                    let receipt = missing_receipt(&identity, &command.policy)?;
                    self.journal.store_terminal_receipt(receipt)?;
                }
                PluginNodeInvocationRecovery::CompleteFromCanonicalProposal => {
                    let record = invocation_record(&head.state, &identity)?;
                    return Ok(PluginTurnResult {
                        last_sequence: head.state.last_sequence,
                        outcome: PluginTurnOutcome::ProposalPendingValidation {
                            invocation_id: identity.invocation_id.clone(),
                            proposal: record.proposal.clone().ok_or(PluginTurnError::Projection)?,
                        },
                    });
                }
                PluginNodeInvocationRecovery::TerminallyFailed => {
                    let record = invocation_record(&head.state, &identity)?;
                    return Ok(PluginTurnResult {
                        last_sequence: head.state.last_sequence,
                        outcome: PluginTurnOutcome::Failed {
                            code: record
                                .failure_code
                                .clone()
                                .ok_or(PluginTurnError::Projection)?,
                        },
                    });
                }
                PluginNodeInvocationRecovery::AmbiguousFailClosed => {
                    let record = invocation_record(&head.state, &identity)?;
                    return Ok(PluginTurnResult {
                        last_sequence: head.state.last_sequence,
                        outcome: PluginTurnOutcome::AmbiguousFailClosed {
                            code: record
                                .failure_code
                                .clone()
                                .ok_or(PluginTurnError::Projection)?,
                        },
                    });
                }
            }
        }
        Err(PluginTurnError::RoundLimit)
    }

    fn validate_and_build_identity(
        &self,
        command: &DrivePluginTurnCommand,
    ) -> Result<PluginNodeInvocationIdentity, PluginTurnError> {
        if command.cancellation_id.is_empty()
            || command.policy.declaration_hash != command.executor.executor_declaration_hash
            || command.policy.max_attempts == 0
            || command.policy.max_attempts > MAX_PLUGIN_NODE_ATTEMPTS
            || !canonical_permissions(&command.policy.required_permissions)
        {
            return Err(PluginTurnError::Projection);
        }
        let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
            return Err(PluginTurnError::Projection);
        };
        let head = self.journal.load()?;
        let execution = head
            .state
            .style_execution
            .as_ref()
            .ok_or(PluginTurnError::Projection)?;
        let mut existing = execution.plugin_node_invocations.values().filter(|record| {
            record.identity.work == command.work && record.identity.executor == command.executor
        });
        let first_existing = existing.next();
        if existing.next().is_some() {
            return Err(PluginTurnError::Projection);
        }
        if let Some(record) = first_existing {
            let recovery = classify_plugin_node_invocation_recovery(
                &head.state,
                &record.identity.invocation_id,
            );
            if recovery_uses_canonical_readable_identity(recovery) {
                if record.identity.plugin_id != *plugin_id
                    || plugin_node_value_hash(&command.input)? != record.identity.input_hash
                {
                    return Err(PluginTurnError::Projection);
                }
                return Ok(record.identity.clone());
            }
        }
        let plugin_command = executor_command(command)?;
        let (invocation_id, invocation_digest) = plugin_node_invocation_identity(&plugin_command)?;
        let causation_event_id = head
            .state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.plugin_node_invocations.get(&invocation_id))
            .map_or(head.last_event_id, |record| {
                record.identity.causation_event_id
            });
        Ok(PluginNodeInvocationIdentity {
            work: command.work.clone(),
            executor: command.executor.clone(),
            configuration_hash: command.executor.adapter_configuration_reference,
            plugin_id: plugin_id.clone(),
            invocation_id,
            invocation_digest,
            input_hash: plugin_node_value_hash(&command.input)?,
            readable_state_hash: plugin_node_value_hash(&command.readable_state)?,
            causation_event_id,
        })
    }

    fn validate_head(
        &self,
        head: &PluginTurnHead,
        command: &DrivePluginTurnCommand,
        identity: &PluginNodeInvocationIdentity,
    ) -> Result<(), PluginTurnError> {
        if head.state.id != command.session_id {
            return Err(PluginTurnError::Projection);
        }
        let execution = head
            .state
            .style_execution
            .as_ref()
            .ok_or(PluginTurnError::Projection)?;
        let contract = execution
            .execution_contract
            .as_deref()
            .ok_or(PluginTurnError::Projection)?;
        let planned = contract
            .node_executors
            .iter()
            .find(|resolution| resolution.node_id == command.work.node_id)
            .ok_or(PluginTurnError::Projection)?;
        let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
            return Err(PluginTurnError::Projection);
        };
        let recovery =
            classify_plugin_node_invocation_recovery(&head.state, &identity.invocation_id);
        if planned != &command.executor
            || command.work.run_id != contract.run_id
            || command.executor.boundary != SessionNodeExecutorBoundary::PluginHost
            || command.executor.node_id != command.work.node_id
            || command.policy.declaration_hash != command.executor.executor_declaration_hash
            || (!recovery_uses_canonical_readable_identity(recovery)
                && !head.state.plugins.activated_plugin_ids.contains(plugin_id))
            || plugin_node_value_hash(&command.input)? != identity.input_hash
        {
            return Err(PluginTurnError::Projection);
        }
        if !recovery_uses_canonical_readable_identity(recovery) {
            validate_canonical_plugin_state_projection(
                &head.state,
                identity,
                &command.readable_state,
                self.state_scope,
            )?;
            if plugin_node_value_hash(&command.readable_state)? != identity.readable_state_hash {
                return Err(PluginTurnError::Projection);
            }
        }
        if let Some(record) = execution
            .plugin_node_invocations
            .get(&identity.invocation_id)
            && record.identity != *identity
        {
            return Err(PluginTurnError::Projection);
        }
        Ok(())
    }

    async fn invoke_once_and_store(
        &self,
        command: &DrivePluginTurnCommand,
        identity: &PluginNodeInvocationIdentity,
    ) -> Result<(), PluginTurnError> {
        if self
            .journal
            .cancellation_requested(&command.cancellation_id)?
        {
            self.journal
                .store_terminal_receipt(PluginNodeTerminalReceipt::seal(
                    identity.clone(),
                    PluginNodeTerminalReceiptOutcome::Failed {
                        code: String::from("cancelled_before_plugin_boundary"),
                        diagnostic: None,
                        attempts: 1,
                    },
                )?)?;
            return Ok(());
        }
        let result = self
            .executor
            .execute_plugin_node(executor_command(command)?)
            .await;
        let outcome = match result {
            Ok(proposal) => {
                let attempts = proposal.attempts;
                if proposal.invocation_id != identity.invocation_id
                    || proposal.invocation_digest != identity.invocation_digest
                    || attempts == 0
                    || attempts > command.policy.max_attempts
                {
                    execution_error_outcome(
                        &command.policy,
                        "invalid_plugin_outcome",
                        attempts.max(1),
                    )
                } else {
                    match canonical_proposal(proposal) {
                        Ok(proposal) => PluginNodeTerminalReceiptOutcome::Completed {
                            proposal: Box::new(proposal),
                            attempts,
                        },
                        Err(_) => execution_error_outcome(
                            &command.policy,
                            "invalid_plugin_outcome",
                            attempts,
                        ),
                    }
                }
            }
            Err(error) => execution_error_receipt_outcome(&command.policy, identity, error)?,
        };
        self.journal
            .store_terminal_receipt(PluginNodeTerminalReceipt::seal(identity.clone(), outcome)?)?;
        Ok(())
    }

    fn commit_receipt(
        &self,
        head: PluginTurnHead,
        prior_event_id: EventId,
        identity: &PluginNodeInvocationIdentity,
        receipt: PluginNodeTerminalReceipt,
    ) -> Result<PluginTurnHead, PluginTurnError> {
        receipt.validate()?;
        if receipt.identity != *identity {
            return Err(PluginTurnError::InvalidReceipt);
        }
        let payload = match receipt.outcome {
            PluginNodeTerminalReceiptOutcome::Completed { proposal, attempts } => {
                RuntimeCommittedEvent::PluginNodeInvocationCompleted(Box::new(
                    PluginNodeInvocationCompletedEvent {
                        identity: identity.clone(),
                        prior_event_id,
                        proposal: *proposal,
                        attempts,
                    },
                ))
            }
            PluginNodeTerminalReceiptOutcome::Failed {
                code,
                diagnostic,
                attempts,
            } => RuntimeCommittedEvent::PluginNodeInvocationFailed(Box::new(
                PluginNodeInvocationFailedEvent {
                    identity: identity.clone(),
                    prior_event_id,
                    code,
                    diagnostic,
                    attempts,
                },
            )),
            PluginNodeTerminalReceiptOutcome::Ambiguous {
                code,
                diagnostic,
                attempts,
            } => RuntimeCommittedEvent::PluginNodeInvocationAmbiguous(Box::new(
                PluginNodeInvocationAmbiguousEvent {
                    identity: identity.clone(),
                    prior_event_id,
                    code,
                    diagnostic,
                    attempts,
                },
            )),
        };
        self.commit(head, payload)
    }

    fn commit_failed(
        &self,
        head: PluginTurnHead,
        identity: &PluginNodeInvocationIdentity,
        prior_event_id: EventId,
        code: &str,
        attempts: u8,
    ) -> Result<PluginTurnHead, PluginTurnError> {
        self.commit(
            head,
            RuntimeCommittedEvent::PluginNodeInvocationFailed(Box::new(
                PluginNodeInvocationFailedEvent {
                    identity: identity.clone(),
                    prior_event_id,
                    code: code.to_owned(),
                    diagnostic: None,
                    attempts,
                },
            )),
        )
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "consuming the reducer-validated head prevents accidental reuse after append"
    )]
    fn commit(
        &self,
        mut head: PluginTurnHead,
        payload: RuntimeCommittedEvent,
    ) -> Result<PluginTurnHead, PluginTurnError> {
        const MAX_CONCURRENT_APPEND_RECONCILIATIONS: u8 = 8;
        for reconciliation in 0..=MAX_CONCURRENT_APPEND_RECONCILIATIONS {
            let identity = self.journal.allocate_identity()?;
            let sequence = head
                .state
                .last_sequence
                .checked_next()
                .map_err(|_| PluginTurnError::Sequence)?;
            let event = seal_event(&head, sequence, identity, payload.clone())?;
            let next_state = reduce(Some(head.state.clone()), &event)?;
            match self.journal.append(
                PluginTurnAppendPosition {
                    sequence: head.state.last_sequence,
                    event_id: head.last_event_id,
                },
                event,
            ) {
                Ok(()) => {
                    return Ok(PluginTurnHead {
                        state: next_state,
                        last_event_id: identity.event_id,
                    });
                }
                Err(error)
                    if error.code == "append_conflict"
                        && reconciliation < MAX_CONCURRENT_APPEND_RECONCILIATIONS =>
                {
                    let refreshed = self.journal.load()?;
                    if refreshed.state.last_sequence == head.state.last_sequence
                        && refreshed.last_event_id == head.last_event_id
                    {
                        return Err(error.into());
                    }
                    head = refreshed;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(plugin_turn_journal_error("append_conflict").into())
    }
}

fn validate_canonical_plugin_state_projection(
    state: &SessionState,
    identity: &PluginNodeInvocationIdentity,
    readable_state: &serde_json::Value,
    state_scope: PersistenceStateScope,
) -> Result<(), PluginTurnError> {
    let object = readable_state
        .as_object()
        .ok_or(PluginTurnError::Projection)?;
    match state_scope {
        PersistenceStateScope::Invocation => {
            if object.contains_key("$plugin_state") {
                Err(PluginTurnError::Projection)
            } else {
                Ok(())
            }
        }
        PersistenceStateScope::Session => {
            let prior = derive_plugin_node_state_prior(state, identity, state_scope)?;
            match (prior.state_hash, object.get("$plugin_state")) {
                (None, None) => Ok(()),
                (Some(expected), Some(raw)) if plugin_node_value_hash(raw)? == expected => Ok(()),
                _ => Err(PluginTurnError::Projection),
            }
        }
        PersistenceStateScope::ModelCall
        | PersistenceStateScope::Turn
        | PersistenceStateScope::Project
        | PersistenceStateScope::User
        | PersistenceStateScope::Runtime => Err(PluginTurnError::Projection),
    }
}

fn executor_command(
    command: &DrivePluginTurnCommand,
) -> Result<ExecutePluginNodeCommand, PluginTurnError> {
    let source = match &command.executor.source {
        SessionNodeExecutorSource::Runtime => NodeExecutorSource::Runtime,
        SessionNodeExecutorSource::Plugin { plugin_id } => NodeExecutorSource::Plugin {
            plugin_id: plugin_id.clone(),
        },
    };
    let boundary = match command.executor.boundary {
        SessionNodeExecutorBoundary::RuntimeLogic => NodeExecutorBoundary::RuntimeLogic,
        SessionNodeExecutorBoundary::PluginHost => NodeExecutorBoundary::PluginHost,
    };
    let executor = ResolvedNodeExecutor {
        node_id: command.executor.node_id.clone(),
        node_kind: command.executor.node_kind.clone(),
        implementation_id: command.executor.executor_id.clone(),
        implementation_version: command.executor.executor_version.clone(),
        source,
        boundary,
        required_capabilities: command
            .executor
            .required_capabilities
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        resolved_capabilities: command
            .executor
            .resolved_capabilities
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        runtime_api_requirement: command.executor.runtime_api_requirement.clone(),
        executor_declaration_hash: command.executor.executor_declaration_hash,
        adapter_configuration_reference: command.executor.adapter_configuration_reference,
    };
    if executor.boundary != NodeExecutorBoundary::PluginHost
        || executor.node_id != command.work.node_id
    {
        return Err(PluginTurnError::Projection);
    }
    Ok(ExecutePluginNodeCommand {
        session_id: command.session_id.to_string(),
        work: command.work.clone(),
        executor,
        adapter_configuration_reference: command.executor.adapter_configuration_reference,
        input: command.input.clone(),
        readable_state: command.readable_state.clone(),
        cancellation_id: command.cancellation_id.clone(),
    })
}

fn authorization_command(
    state: &SessionState,
    command: &DrivePluginTurnCommand,
    identity: &PluginNodeInvocationIdentity,
) -> Result<AuthorizePluginTurnCommand, PluginTurnError> {
    let action = ConsequentialAction::PluginNodeInvocation(PluginNodeInvocationAction {
        plugin_id: identity.plugin_id.clone(),
        executor_id: identity.executor.executor_id.clone(),
        executor_version: identity.executor.executor_version.clone(),
        invocation_id: identity.invocation_id.clone(),
        invocation_digest: identity.invocation_digest,
        declaration_hash: command.policy.declaration_hash,
        external_effects: command.policy.external_effects,
        required_permissions: command.policy.required_permissions.clone(),
    });
    let proposal = ActionProposal {
        id: ProposalId(identity.invocation_id.clone()),
        action,
        style: state.style.clone(),
        workspace: state.workspace.clone(),
        origin: String::from("runtime"),
    };
    let action_digest = proposal
        .digest()
        .map_err(|_| PluginTurnError::Serialization)?;
    Ok(AuthorizePluginTurnCommand {
        session_id: command.session_id,
        identity: identity.clone(),
        policy: command.policy.clone(),
        action_digest,
        proposal,
        executor_source: command.executor.source.clone(),
        declaration_hash: command.executor.executor_declaration_hash,
    })
}

fn canonical_permissions(permissions: &[String]) -> bool {
    permissions.windows(2).all(|pair| pair[0] < pair[1])
        && permissions.iter().all(|permission| {
            !permission.is_empty()
                && permission.len() <= 128
                && permission.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b':' | b'-')
                })
        })
}

fn canonical_proposal(
    proposal: crate::plugin::PluginNodeExecutionProposal,
) -> Result<CanonicalPluginNodeOutcomeProposal, PluginTurnError> {
    let proposed_actions = proposal
        .proposed_actions
        .into_iter()
        .map(|action| {
            Ok(CanonicalPluginNodeActionProposal {
                action_hash: plugin_node_action_hash(&action.kind, &action.payload)?,
                kind: action.kind,
                payload: action.payload,
            })
        })
        .collect::<Result<Vec<_>, PluginTurnError>>()?;
    Ok(CanonicalPluginNodeOutcomeProposal {
        output_hash: plugin_node_value_hash(&proposal.output)?,
        output: proposal.output,
        preserved_state_hash: plugin_node_value_hash(&proposal.preserved_state)?,
        preserved_state: proposal.preserved_state,
        proposed_actions_hash: plugin_node_actions_hash(&proposed_actions)?,
        proposed_actions,
    })
}

fn execution_error_receipt_outcome(
    policy: &PluginNodeInvocationPolicy,
    identity: &PluginNodeInvocationIdentity,
    error: PluginNodeExecutionError,
) -> Result<PluginNodeTerminalReceiptOutcome, PluginTurnError> {
    match error {
        PluginNodeExecutionError::Ambiguous {
            invocation_id,
            invocation_digest,
            ..
        } => {
            if invocation_id != identity.invocation_id
                || invocation_digest != identity.invocation_digest
            {
                return Err(PluginTurnError::InvalidReceipt);
            }
            Ok(PluginNodeTerminalReceiptOutcome::Ambiguous {
                code: String::from("plugin_execution_ambiguous"),
                diagnostic: None,
                attempts: 1,
            })
        }
        PluginNodeExecutionError::InvalidResolution
        | PluginNodeExecutionError::InvalidDeclaration => {
            Ok(PluginNodeTerminalReceiptOutcome::Failed {
                code: String::from("plugin_resolution_invalid"),
                diagnostic: None,
                attempts: 1,
            })
        }
        PluginNodeExecutionError::InvalidOutcome | PluginNodeExecutionError::Data(_) => Ok(
            execution_error_outcome(policy, "plugin_execution_failed", 1),
        ),
    }
}

fn execution_error_outcome(
    policy: &PluginNodeInvocationPolicy,
    code: &str,
    attempts: u8,
) -> PluginNodeTerminalReceiptOutcome {
    if policy.external_effects || !policy.idempotent {
        PluginNodeTerminalReceiptOutcome::Ambiguous {
            code: code.to_owned(),
            diagnostic: None,
            attempts: attempts.clamp(1, MAX_PLUGIN_NODE_ATTEMPTS),
        }
    } else {
        PluginNodeTerminalReceiptOutcome::Failed {
            code: code.to_owned(),
            diagnostic: None,
            attempts: attempts.clamp(1, MAX_PLUGIN_NODE_ATTEMPTS),
        }
    }
}

fn missing_receipt(
    identity: &PluginNodeInvocationIdentity,
    policy: &PluginNodeInvocationPolicy,
) -> Result<PluginNodeTerminalReceipt, PluginTurnError> {
    PluginNodeTerminalReceipt::seal(
        identity.clone(),
        execution_error_outcome(policy, "terminal_receipt_missing", 1),
    )
}

fn invocation_record<'a>(
    state: &'a SessionState,
    identity: &PluginNodeInvocationIdentity,
) -> Result<&'a PluginNodeInvocationRecord, PluginTurnError> {
    let record = state
        .style_execution
        .as_ref()
        .and_then(|execution| {
            execution
                .plugin_node_invocations
                .get(&identity.invocation_id)
        })
        .ok_or(PluginTurnError::Projection)?;
    if record.identity != *identity {
        return Err(PluginTurnError::Projection);
    }
    Ok(record)
}

fn terminal_receipt_hash(
    identity: &PluginNodeInvocationIdentity,
    outcome: &PluginNodeTerminalReceiptOutcome,
) -> Result<ContentHash, PluginTurnError> {
    serde_json::to_vec(&(identity, outcome))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginTurnError::Serialization)
}

fn dispatch_digest(
    identity: &PluginNodeInvocationIdentity,
    authorization_digest: ContentHash,
) -> Result<ContentHash, PluginTurnError> {
    serde_json::to_vec(&(identity, authorization_digest))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginTurnError::Serialization)
}

fn seal_event(
    head: &PluginTurnHead,
    sequence: Sequence,
    identity: PluginTurnEventIdentity,
    payload: RuntimeCommittedEvent,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, PluginTurnError> {
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
            parent_graph_node_id: None,
            origin: EventOrigin {
                subsystem: String::from("runtime"),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts: vec![],
            classification: EventClassification::Committed,
        },
        payload,
    )
    .map_err(|_| PluginTurnError::Event)
}

fn plugin_turn_journal_error(code: &str) -> PluginTurnJournalError {
    PluginTurnJournalError {
        code: code.to_owned(),
    }
}

/// Stable live plugin-node coordinator failure.
#[derive(Debug, Error)]
pub enum PluginTurnError {
    /// Command, persisted plan, active work, hashes, or replay projection drifted.
    #[error("plugin turn projection does not match the immutable command")]
    Projection,
    /// Isolated plugin executor rejected the command before a terminal receipt.
    #[error("plugin node executor failed: {0}")]
    Plugin(#[from] PluginNodeExecutionError),
    /// Pure replay reducer rejected a lifecycle event.
    #[error("plugin turn reducer rejected a lifecycle event: {0}")]
    Reducer(#[from] SessionReducerError),
    /// Journal or durable receipt boundary failed.
    #[error(transparent)]
    Journal(#[from] PluginTurnJournalError),
    /// Authorization boundary failed before dispatch.
    #[error(transparent)]
    Authorization(#[from] PluginTurnAuthorizationError),
    /// Durable terminal receipt was substituted or corrupted.
    #[error("plugin turn terminal receipt is invalid")]
    InvalidReceipt,
    /// Canonical identity material could not be serialized.
    #[error("plugin turn identity serialization failed")]
    Serialization,
    /// Canonical sequence overflowed.
    #[error("plugin turn sequence overflow")]
    Sequence,
    /// Canonical event envelope could not be sealed.
    #[error("plugin turn event could not be sealed")]
    Event,
    /// Bounded coordinator rounds were exhausted.
    #[error("plugin turn coordination round bound was exhausted")]
    RoundLimit,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    };

    use agentmod_event_model::EventOrigin;
    use agentmod_primitives::CausationId;
    use agentmod_runtime_data::{
        cancellation::{RequestRuntimeCancellationDataCommand, RuntimeCancellationControlDataPort},
        fixture_file::{
            CorruptFixtureFileDataRequest, FixtureFileDataPort, ListFixtureDirectoryDataRequest,
        },
        local::{LocalRuntimeDataPort, local_plugin_runtime_data},
    };
    use agentmod_session_style_sdk::{BuiltInStyle, CompiledSessionStyle};
    use uuid::Uuid;

    use crate::{
        plugin::{
            PluginNodeActionProposal, PluginNodeExecutionProposal, plugin_node_invocation_identity,
        },
        session::{
            PluginSetActivatedEvent, SessionCreatedEvent, StyleExecutionContract,
            StyleExecutionInitializedEvent, StyleNodeEnteredEvent, replay,
        },
        style_executor::tests::binding,
    };

    use super::*;

    #[test]
    fn live_plugin_branch_context_is_exact_and_never_claims_shared_serialization() {
        let mut work = NodeWorkIdentity {
            run_id: String::from("run"),
            node_id: String::from("plugin"),
            branch_path: vec![String::from("branch-a")],
            attempt: 1,
            loop_iteration: 0,
            step: 2,
        };
        let branch = BranchWriteContext {
            branch_id: String::from("branch-a"),
            stable_order: 1,
            serialized_shared_write: false,
        };
        assert!(validate_live_plugin_branch_context(&work, Some(&branch)).is_ok());

        let mut substituted = branch.clone();
        substituted.branch_id = String::from("branch-b");
        assert!(matches!(
            validate_live_plugin_branch_context(&work, Some(&substituted)),
            Err(PluginNodeTurnRuntimeError::InvalidBranchContext)
        ));
        let mut forged_serialization = branch.clone();
        forged_serialization.serialized_shared_write = true;
        assert!(matches!(
            validate_live_plugin_branch_context(&work, Some(&forged_serialization)),
            Err(PluginNodeTurnRuntimeError::InvalidBranchContext)
        ));
        assert!(matches!(
            validate_live_plugin_branch_context(&work, None),
            Err(PluginNodeTurnRuntimeError::InvalidBranchContext)
        ));

        work.branch_path.clear();
        assert!(validate_live_plugin_branch_context(&work, None).is_ok());
        assert!(matches!(
            validate_live_plugin_branch_context(&work, Some(&branch)),
            Err(PluginNodeTurnRuntimeError::InvalidBranchContext)
        ));
    }

    #[test]
    fn prior_plugin_state_merges_only_under_reserved_non_graph_key() {
        let first = merge_prior_plugin_state(
            serde_json::json!({"declared": 1}),
            PriorPluginNodeState::None,
        )
        .expect("first session invocation");
        assert!(first.get("$plugin_state").is_none());
        let state = serde_json::json!({"cursor":"secret-prior"});
        let state_hash = plugin_node_value_hash(&state).expect("state hash");
        let merged = merge_prior_plugin_state(
            serde_json::json!({"declared": 1}),
            PriorPluginNodeState::Loaded {
                generation: 3,
                state_hash,
                state: state.clone(),
            },
        )
        .expect("merge");
        assert_eq!(merged["declared"], 1);
        assert_eq!(merged["$plugin_state"], state);
        assert!(matches!(
            merge_prior_plugin_state(
                serde_json::json!({"$plugin_state": "collision"}),
                PriorPluginNodeState::None
            ),
            Err(PluginNodeTurnRuntimeError::PluginStateCollision)
        ));
        assert!(matches!(
            merge_prior_plugin_state(serde_json::Value::Null, PriorPluginNodeState::None),
            Err(PluginNodeTurnRuntimeError::InvalidReadableState)
        ));
    }

    #[test]
    fn declaration_state_scope_is_exact_and_fail_closed() {
        assert_eq!(
            parse_state_scope("invocation").expect("invocation"),
            PersistenceStateScope::Invocation
        );
        assert_eq!(
            parse_state_scope("session").expect("session"),
            PersistenceStateScope::Session
        );
        for scope in ["model_call", "turn", "project", "user", "runtime"] {
            assert!(matches!(
                parse_state_scope(scope),
                Err(PluginNodeTurnRuntimeError::UnsupportedStateScope)
            ));
        }
        assert!(matches!(
            parse_state_scope("SESSION"),
            Err(PluginNodeTurnRuntimeError::InvalidResolution)
        ));
    }

    #[tokio::test]
    async fn invocation_scope_rejects_injected_plugin_state_before_dispatch() {
        let (journal, mut command) = fixture();
        command.readable_state = serde_json::json!({
            "classification": "internal",
            "$plugin_state": {"cursor":"forged"}
        });
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        assert!(matches!(
            coordinator(
                journal,
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor)
            )
            .drive(command)
            .await,
            Err(PluginTurnError::Projection)
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[derive(Clone, Debug)]
    struct FailureCut {
        event_type: String,
        after_append: bool,
    }

    struct ConcurrentEventCut {
        before_event_type: String,
        payloads: Vec<RuntimeCommittedEvent>,
    }

    #[derive(Clone)]
    struct MockJournal {
        inner: Arc<MockJournalInner>,
    }

    struct MockJournalInner {
        events: Mutex<Vec<EventEnvelope<RuntimeCommittedEvent>>>,
        receipts: Mutex<HashMap<String, PluginNodeTerminalReceipt>>,
        next_identity: AtomicU64,
        failure: Mutex<Option<FailureCut>>,
        concurrent_event: Mutex<Option<ConcurrentEventCut>>,
        cancelled: AtomicBool,
    }

    impl MockJournal {
        fn new(events: Vec<EventEnvelope<RuntimeCommittedEvent>>) -> Self {
            Self {
                inner: Arc::new(MockJournalInner {
                    events: Mutex::new(events),
                    receipts: Mutex::new(HashMap::new()),
                    next_identity: AtomicU64::new(1_000),
                    failure: Mutex::new(None),
                    concurrent_event: Mutex::new(None),
                    cancelled: AtomicBool::new(false),
                }),
            }
        }

        fn fail_once(&self, event_type: &str, after_append: bool) {
            *self.inner.failure.lock().expect("failure") = Some(FailureCut {
                event_type: event_type.to_owned(),
                after_append,
            });
        }

        fn cancel(&self) {
            self.inner.cancelled.store(true, Ordering::SeqCst);
        }

        fn inject_concurrent_events(
            &self,
            before_event_type: &str,
            payloads: Vec<RuntimeCommittedEvent>,
        ) {
            *self
                .inner
                .concurrent_event
                .lock()
                .expect("concurrent event") = Some(ConcurrentEventCut {
                before_event_type: before_event_type.to_owned(),
                payloads,
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

        fn inject_receipt(&self, receipt: PluginNodeTerminalReceipt) {
            self.inner
                .receipts
                .lock()
                .expect("receipts")
                .insert(receipt.identity.invocation_id.clone(), receipt);
        }

        fn commit_external_event(&self, payload: RuntimeCommittedEvent) {
            let head = self.load().expect("external event head");
            let identity = self.allocate_identity().expect("external event identity");
            let sequence = head
                .state
                .last_sequence
                .checked_next()
                .expect("external event sequence");
            let event = seal_event(&head, sequence, identity, payload).expect("external event");
            self.append(
                PluginTurnAppendPosition {
                    sequence: head.state.last_sequence,
                    event_id: head.last_event_id,
                },
                event,
            )
            .expect("external event append");
        }
    }

    impl PluginTurnJournal for MockJournal {
        fn load(&self) -> Result<PluginTurnHead, PluginTurnJournalError> {
            let events = self.inner.events.lock().expect("events");
            let state = replay(&*events).map_err(|error| PluginTurnJournalError {
                code: error.to_string(),
            })?;
            Ok(PluginTurnHead {
                state,
                last_event_id: events.last().expect("seeded").metadata.event_id,
            })
        }

        fn allocate_identity(&self) -> Result<PluginTurnEventIdentity, PluginTurnJournalError> {
            let value = self.inner.next_identity.fetch_add(1, Ordering::SeqCst);
            Ok(PluginTurnEventIdentity {
                event_id: EventId::from_uuid(Uuid::from_u128(u128::from(value))),
                timestamp: TimestampMillis::new(i64::try_from(value).expect("timestamp")),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(9)),
            })
        }

        fn append(
            &self,
            expected: PluginTurnAppendPosition,
            event: EventEnvelope<RuntimeCommittedEvent>,
        ) -> Result<(), PluginTurnJournalError> {
            let mut events = self.inner.events.lock().expect("events");
            let concurrent = {
                let mut cut = self
                    .inner
                    .concurrent_event
                    .lock()
                    .expect("concurrent event");
                if cut
                    .as_ref()
                    .is_some_and(|cut| cut.before_event_type == event.metadata.event_type)
                {
                    cut.take()
                } else {
                    None
                }
            };
            if let Some(ConcurrentEventCut { payloads, .. }) = concurrent {
                for payload in payloads {
                    let state = replay(&*events).map_err(|error| PluginTurnJournalError {
                        code: error.to_string(),
                    })?;
                    let head = PluginTurnHead {
                        state,
                        last_event_id: events.last().expect("seeded").metadata.event_id,
                    };
                    let value = self.inner.next_identity.fetch_add(1, Ordering::SeqCst);
                    let identity = PluginTurnEventIdentity {
                        event_id: EventId::from_uuid(Uuid::from_u128(u128::from(value))),
                        timestamp: TimestampMillis::new(i64::try_from(value).expect("timestamp")),
                        correlation_id: CorrelationId::from_uuid(Uuid::from_u128(9)),
                    };
                    let sequence = head
                        .state
                        .last_sequence
                        .checked_next()
                        .map_err(|_| plugin_turn_journal_error("sequence_overflow"))?;
                    let concurrent = seal_event(&head, sequence, identity, payload)
                        .map_err(|error| plugin_turn_journal_error(&error.to_string()))?;
                    reduce(Some(head.state), &concurrent)
                        .map_err(|error| plugin_turn_journal_error(&error.to_string()))?;
                    events.push(concurrent);
                }
            }
            let head = events.last().expect("seeded");
            if head.metadata.sequence != expected.sequence
                || head.metadata.event_id != expected.event_id
                || event.metadata.sequence.get() != events.len() as u64 + 1
            {
                return Err(PluginTurnJournalError {
                    code: String::from("append_conflict"),
                });
            }
            let cut = self
                .inner
                .failure
                .lock()
                .expect("failure")
                .as_ref()
                .is_some_and(|cut| cut.event_type == event.metadata.event_type);
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
                return Err(PluginTurnJournalError {
                    code: String::from("append_conflict"),
                });
            }
            events.push(event);
            if after_append {
                self.inner.failure.lock().expect("failure").take();
                return Err(PluginTurnJournalError {
                    code: String::from("ambiguous_append"),
                });
            }
            Ok(())
        }

        fn terminal_receipt(
            &self,
            invocation_id: &str,
        ) -> Result<Option<PluginNodeTerminalReceipt>, PluginTurnJournalError> {
            Ok(self
                .inner
                .receipts
                .lock()
                .expect("receipts")
                .get(invocation_id)
                .cloned())
        }

        fn store_terminal_receipt(
            &self,
            receipt: PluginNodeTerminalReceipt,
        ) -> Result<(), PluginTurnJournalError> {
            let mut receipts = self.inner.receipts.lock().expect("receipts");
            match receipts.get(&receipt.identity.invocation_id) {
                Some(existing) if existing == &receipt => Ok(()),
                Some(_) => Err(PluginTurnJournalError {
                    code: String::from("receipt_conflict"),
                }),
                None => {
                    receipts.insert(receipt.identity.invocation_id.clone(), receipt);
                    Ok(())
                }
            }
        }

        fn cancellation_requested(
            &self,
            _cancellation_id: &str,
        ) -> Result<bool, PluginTurnJournalError> {
            Ok(self.inner.cancelled.load(Ordering::SeqCst))
        }
    }

    #[derive(Default)]
    struct MockAuthorization {
        calls: AtomicU64,
        reject: AtomicBool,
        ask: AtomicBool,
        observed: Mutex<Vec<AuthorizePluginTurnCommand>>,
    }

    #[async_trait]
    impl PluginTurnAuthorizationPort for MockAuthorization {
        async fn authorize_plugin_turn(
            &self,
            command: AuthorizePluginTurnCommand,
        ) -> Result<PluginTurnAuthorization, PluginTurnAuthorizationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                command.proposal.digest().expect("proposal digest"),
                command.action_digest
            );
            assert_eq!(command.declaration_hash, command.policy.declaration_hash);
            assert_eq!(
                command.executor_source,
                SessionNodeExecutorSource::Plugin {
                    plugin_id: String::from("fixture.plugin-node")
                }
            );
            let ConsequentialAction::PluginNodeInvocation(action) = &command.proposal.action else {
                panic!("plugin invocation action")
            };
            assert_eq!(action.plugin_id, command.identity.plugin_id);
            assert_eq!(action.executor_id, command.identity.executor.executor_id);
            assert_eq!(
                action.declaration_hash,
                command.identity.executor.executor_declaration_hash
            );
            assert_eq!(action.invocation_digest, command.identity.invocation_digest);
            self.observed
                .lock()
                .expect("observed")
                .push(command.clone());
            if self.reject.load(Ordering::SeqCst) {
                return Err(PluginTurnAuthorizationError::Denied {
                    reason: String::from("policy_denied"),
                });
            }
            if self.ask.load(Ordering::SeqCst) {
                return Err(PluginTurnAuthorizationError::ApprovalRequired {
                    proposal: Box::new(command.proposal),
                    reason: String::from("approval_required"),
                    continuation: None,
                });
            }
            Ok(PluginTurnAuthorization {
                authorization_digest: ContentHash::digest(
                    command.action_digest.to_hex().as_bytes(),
                ),
            })
        }
    }

    #[derive(Clone, Copy)]
    enum ExecutorBehavior {
        Success,
        InvalidAction,
        Unavailable,
        Timeout,
        Ambiguous,
    }

    struct MockExecutor {
        calls: AtomicU64,
        behavior: ExecutorBehavior,
        readable_states: Mutex<Vec<serde_json::Value>>,
    }

    struct MockCancellation {
        calls: AtomicU64,
        outcome: Mutex<Result<LivePluginNodeCancellationReceipt, ExactPluginCancellationError>>,
    }

    #[async_trait]
    impl ExactPluginCancellationPort for MockCancellation {
        async fn cancel_exact_plugin_invocation(
            &self,
            _command: CancelExactPluginInvocationCommand,
        ) -> Result<LivePluginNodeCancellationReceipt, ExactPluginCancellationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcome.lock().expect("cancellation outcome").clone()
        }
    }

    impl MockExecutor {
        fn new(behavior: ExecutorBehavior) -> Self {
            Self {
                calls: AtomicU64::new(0),
                behavior,
                readable_states: Mutex::new(Vec::new()),
            }
        }

        fn readable_states(&self) -> Vec<serde_json::Value> {
            self.readable_states
                .lock()
                .expect("readable states")
                .clone()
        }
    }

    #[async_trait]
    impl PluginNodeExecutorLogicPort for MockExecutor {
        async fn execute_plugin_node(
            &self,
            command: ExecutePluginNodeCommand,
        ) -> Result<PluginNodeExecutionProposal, PluginNodeExecutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.readable_states
                .lock()
                .expect("readable states")
                .push(command.readable_state.clone());
            let (invocation_id, invocation_digest) = plugin_node_invocation_identity(&command)?;
            match self.behavior {
                ExecutorBehavior::Success => Ok(PluginNodeExecutionProposal {
                    invocation_id,
                    invocation_digest,
                    output: serde_json::json!({"echo": 42}),
                    preserved_state: serde_json::json!({"cursor": 1}),
                    proposed_actions: vec![PluginNodeActionProposal {
                        kind: String::from("emit_event"),
                        payload: serde_json::json!({"event_type":"user.plugin_ready"}),
                    }],
                    attempts: 1,
                }),
                ExecutorBehavior::InvalidAction => Ok(PluginNodeExecutionProposal {
                    invocation_id,
                    invocation_digest,
                    output: serde_json::json!({"echo": 42}),
                    preserved_state: serde_json::Value::Null,
                    proposed_actions: vec![PluginNodeActionProposal {
                        kind: String::from("FORGED_LIFECYCLE"),
                        payload: serde_json::json!({}),
                    }],
                    attempts: 1,
                }),
                ExecutorBehavior::Unavailable => Err(PluginNodeExecutionError::Data(
                    agentmod_runtime_data::plugin::PluginDataError::Unavailable,
                )),
                ExecutorBehavior::Timeout => Err(PluginNodeExecutionError::Data(
                    agentmod_runtime_data::plugin::PluginDataError::Rejected {
                        operation: String::from("invoke_node_executor"),
                        code: String::from("timeout"),
                        retryable: false,
                    },
                )),
                ExecutorBehavior::Ambiguous => Err(PluginNodeExecutionError::Ambiguous {
                    plugin_id: String::from("fixture.plugin-node"),
                    executor_id: String::from("fixture.plugin-node"),
                    invocation_id,
                    invocation_digest,
                }),
            }
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(1))
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(100 + u128::from(sequence))),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(8)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(7)),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            payload,
        )
        .expect("event")
    }

    fn execution_contract(
        binding: &crate::session::SessionStyleBinding,
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

    fn fixture() -> (MockJournal, DrivePluginTurnCommand) {
        let mut style_binding = binding(BuiltInStyle::PersistentChat);
        let compiled: CompiledSessionStyle =
            serde_json::from_str(&style_binding.compiled_style_json).expect("compiled style");
        let node_id = compiled.graph.nodes[compiled.graph.entry_index].id.clone();
        let plan = style_binding.execution_plan.as_mut().expect("plan");
        let executor = plan
            .nodes
            .iter_mut()
            .find(|resolution| resolution.node_id == node_id)
            .expect("entry executor");
        executor.executor_id = String::from("fixture.plugin-node");
        executor.executor_version = String::from("2.1.0");
        executor.source = SessionNodeExecutorSource::Plugin {
            plugin_id: String::from("fixture.plugin-node"),
        };
        executor.boundary = SessionNodeExecutorBoundary::PluginHost;
        executor.executor_declaration_hash = ContentHash::digest(b"fixture declaration");
        let executor = executor.clone();
        style_binding.execution_plan_hash = Some(ContentHash::digest(
            &serde_json::to_vec(plan).expect("plan"),
        ));
        let contract = execution_contract(&style_binding, &compiled.graph);
        let work = NodeWorkIdentity {
            run_id: contract.run_id.clone(),
            node_id: node_id.clone(),
            branch_path: vec![],
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        };
        let plugin_set_hash = style_binding.plugin_set_hash;
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
                RuntimeCommittedEvent::PluginSetActivated(PluginSetActivatedEvent {
                    plugin_ids: vec![String::from("fixture.plugin-node")],
                    plugin_set_hash,
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(compiled.graph),
                        input_reference: None,
                        execution_contract: Some(Box::new(contract)),
                    },
                )),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id,
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
        ];
        let policy = PluginNodeInvocationPolicy {
            declaration_hash: executor.executor_declaration_hash,
            idempotent: true,
            external_effects: false,
            max_attempts: 1,
            required_permissions: vec![
                String::from("network.api.example"),
                String::from("tool.filesystem.read"),
            ],
        };
        (
            MockJournal::new(events),
            DrivePluginTurnCommand {
                session_id: session_id(),
                work,
                executor,
                input: serde_json::json!({"value": 42}),
                readable_state: serde_json::json!({"classification":"internal"}),
                cancellation_id: String::from("cancel-plugin-node"),
                policy,
            },
        )
    }

    fn coordinator(
        journal: MockJournal,
        authorization: Arc<MockAuthorization>,
        executor: Arc<MockExecutor>,
    ) -> PluginTurnCoordinator<MockJournal, MockAuthorization, MockExecutor> {
        PluginTurnCoordinator::new(journal, authorization, executor)
    }

    fn coordinator_with_scope(
        journal: MockJournal,
        authorization: Arc<MockAuthorization>,
        executor: Arc<MockExecutor>,
        state_scope: PersistenceStateScope,
    ) -> PluginTurnCoordinator<MockJournal, MockAuthorization, MockExecutor> {
        PluginTurnCoordinator::new_with_state_scope(journal, authorization, executor, state_scope)
    }

    fn expected_identity(
        journal: &MockJournal,
        command: &DrivePluginTurnCommand,
    ) -> PluginNodeInvocationIdentity {
        let plugin_command = executor_command(command).expect("plugin command");
        let (invocation_id, invocation_digest) =
            plugin_node_invocation_identity(&plugin_command).expect("identity");
        let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
            panic!("plugin source")
        };
        PluginNodeInvocationIdentity {
            work: command.work.clone(),
            executor: command.executor.clone(),
            configuration_hash: command.executor.adapter_configuration_reference,
            plugin_id: plugin_id.clone(),
            invocation_id,
            invocation_digest,
            input_hash: plugin_node_value_hash(&command.input).expect("input hash"),
            readable_state_hash: plugin_node_value_hash(&command.readable_state)
                .expect("state hash"),
            causation_event_id: journal.load().expect("head").last_event_id,
        }
    }

    fn cancellation_command(
        identity: &PluginNodeInvocationIdentity,
    ) -> CancelExactPluginInvocationCommand {
        CancelExactPluginInvocationCommand {
            target: plugin_invocation_cancellation_target(
                &session_id().to_string(),
                &identity.work.run_id,
                &identity.plugin_id,
                "1.0.0",
                &identity.invocation_id,
                &identity.executor.executor_id,
                identity.executor.executor_declaration_hash,
                ContentHash::digest(b"request"),
            )
            .expect("target"),
            reason_code: String::from("parallel_branch_cancelled"),
            nonce: String::from("nonce-1"),
            idempotency_key: String::from("cancel-once-1"),
            cancellation_id: String::from("cancel-plugin-node"),
        }
    }

    fn cancellation_receipt(
        command: &CancelExactPluginInvocationCommand,
        status: LivePluginNodeHostCancellationStatus,
    ) -> LivePluginNodeCancellationReceipt {
        LivePluginNodeCancellationReceipt {
            target: command.target.clone(),
            reason_code: command.reason_code.clone(),
            action_digest: ContentHash::digest(b"cancel action"),
            nonce: command.nonce.clone(),
            idempotency_key: command.idempotency_key.clone(),
            cancellation_id: command.cancellation_id.clone(),
            status,
            receipt_id: String::from("cancel-receipt-1"),
            receipt_digest: ContentHash::digest(b"cancel receipt"),
        }
    }

    fn journal_with_invocation_state(
        state: PluginNodeInvocationState,
    ) -> (
        MockJournal,
        DrivePluginTurnCommand,
        PluginNodeInvocationIdentity,
    ) {
        let (journal, command) = fixture();
        let identity = expected_identity(&journal, &command);
        let mut head = commit_plugin_event(
            &journal,
            journal.load().expect("head"),
            RuntimeCommittedEvent::PluginNodeInvocationProposed(Box::new(
                PluginNodeInvocationProposedEvent {
                    identity: identity.clone(),
                },
            )),
        )
        .expect("proposed");
        if state == PluginNodeInvocationState::Proposed {
            return (journal, command, identity);
        }
        let authorization_digest = ContentHash::digest(b"authorization");
        let prior_event_id = invocation_record(&head.state, &identity)
            .expect("record")
            .latest_event_id;
        head = commit_plugin_event(
            &journal,
            head,
            RuntimeCommittedEvent::PluginNodeInvocationAuthorized(Box::new(
                PluginNodeInvocationAuthorizedEvent {
                    identity: identity.clone(),
                    prior_event_id,
                    authorization_digest,
                },
            )),
        )
        .expect("authorized");
        if state == PluginNodeInvocationState::Authorized {
            return (journal, command, identity);
        }
        let prior_event_id = invocation_record(&head.state, &identity)
            .expect("record")
            .latest_event_id;
        commit_plugin_event(
            &journal,
            head,
            RuntimeCommittedEvent::PluginNodeInvocationDispatched(Box::new(
                PluginNodeInvocationDispatchedEvent {
                    identity: identity.clone(),
                    prior_event_id,
                    authorization_digest,
                    dispatch_digest: ContentHash::digest(b"dispatch"),
                },
            )),
        )
        .expect("dispatched");
        (journal, command, identity)
    }

    #[tokio::test]
    async fn cancellation_before_dispatch_is_canonical_and_never_calls_host() {
        for state in [
            PluginNodeInvocationState::Proposed,
            PluginNodeInvocationState::Authorized,
        ] {
            let (journal, command, identity) = journal_with_invocation_state(state);
            let cancellation = MockCancellation {
                calls: AtomicU64::new(0),
                outcome: Mutex::new(Err(ExactPluginCancellationError::Unconfirmed)),
            };
            let result = cancel_and_reconcile_plugin_invocation(
                &journal,
                &cancellation,
                command.session_id,
                identity.clone(),
                cancellation_command(&identity),
            )
            .await
            .expect("cancel before dispatch");
            assert!(matches!(
                result,
                LivePluginNodeCancellationOutcome::CancelledBeforeDispatch { ref invocation }
                    if invocation.state == PluginNodeInvocationState::Failed
                        && invocation.attempts == 0
            ));
            assert_eq!(cancellation.calls.load(Ordering::SeqCst), 0);

            let duplicate = cancel_and_reconcile_plugin_invocation(
                &journal,
                &cancellation,
                command.session_id,
                identity.clone(),
                cancellation_command(&identity),
            )
            .await
            .expect("duplicate cancellation");
            assert!(matches!(
                duplicate,
                LivePluginNodeCancellationOutcome::AlreadyTerminal { .. }
            ));
            assert_eq!(cancellation.calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn dispatched_cancellation_reconciles_only_exact_terminal_receipt() {
        let (journal, command, identity) =
            journal_with_invocation_state(PluginNodeInvocationState::Dispatched);
        let cancel_command = cancellation_command(&identity);
        let cancellation = MockCancellation {
            calls: AtomicU64::new(0),
            outcome: Mutex::new(Ok(cancellation_receipt(
                &cancel_command,
                LivePluginNodeHostCancellationStatus::AlreadyTerminal,
            ))),
        };
        journal.inject_receipt(
            PluginNodeTerminalReceipt::seal(
                identity.clone(),
                PluginNodeTerminalReceiptOutcome::Failed {
                    code: String::from("cancelled_by_plugin_host"),
                    diagnostic: None,
                    attempts: 1,
                },
            )
            .expect("terminal receipt"),
        );
        let result = cancel_and_reconcile_plugin_invocation(
            &journal,
            &cancellation,
            command.session_id,
            identity,
            cancel_command,
        )
        .await
        .expect("reconciled cancellation");
        assert!(matches!(
            result,
            LivePluginNodeCancellationOutcome::TerminalReceiptReconciled {
                ref invocation,
                ref receipt,
            } if invocation.state == PluginNodeInvocationState::Failed
                && receipt.status == LivePluginNodeHostCancellationStatus::AlreadyTerminal
        ));
        assert_eq!(cancellation.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dispatched_cancellation_without_terminal_proof_is_ambiguous_and_never_retried() {
        for outcome in [
            Ok(LivePluginNodeHostCancellationStatus::Signalled),
            Err(ExactPluginCancellationError::Unconfirmed),
        ] {
            let (journal, command, identity) =
                journal_with_invocation_state(PluginNodeInvocationState::Dispatched);
            let cancel_command = cancellation_command(&identity);
            let host_outcome = outcome.map(|status| cancellation_receipt(&cancel_command, status));
            let cancellation = MockCancellation {
                calls: AtomicU64::new(0),
                outcome: Mutex::new(host_outcome),
            };
            let first = cancel_and_reconcile_plugin_invocation(
                &journal,
                &cancellation,
                command.session_id,
                identity.clone(),
                cancel_command.clone(),
            )
            .await
            .expect("ambiguous cancellation");
            assert!(matches!(
                first,
                LivePluginNodeCancellationOutcome::AmbiguousNoProof { ref invocation, .. }
                    if invocation.state == PluginNodeInvocationState::Ambiguous
            ));
            let duplicate = cancel_and_reconcile_plugin_invocation(
                &journal,
                &cancellation,
                command.session_id,
                identity,
                cancel_command,
            )
            .await
            .expect("duplicate cancellation");
            assert!(matches!(
                duplicate,
                LivePluginNodeCancellationOutcome::AlreadyTerminal { .. }
            ));
            assert_eq!(cancellation.calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn cancellation_rejects_cross_session_and_invocation_substitution() {
        let (journal, command, identity) =
            journal_with_invocation_state(PluginNodeInvocationState::Dispatched);
        let cancellation = MockCancellation {
            calls: AtomicU64::new(0),
            outcome: Mutex::new(Err(ExactPluginCancellationError::Invalid)),
        };
        let mut substituted = identity.clone();
        substituted.invocation_id.push_str("-other");
        assert!(matches!(
            cancel_and_reconcile_plugin_invocation(
                &journal,
                &cancellation,
                command.session_id,
                substituted.clone(),
                cancellation_command(&substituted),
            )
            .await,
            Err(PluginNodeTurnRuntimeError::InvalidCancellation)
        ));
        assert!(matches!(
            cancel_and_reconcile_plugin_invocation(
                &journal,
                &cancellation,
                SessionId::from_uuid(Uuid::from_u128(999)),
                identity.clone(),
                cancellation_command(&identity),
            )
            .await,
            Err(PluginNodeTurnRuntimeError::InvalidCancellation)
        ));
        assert_eq!(cancellation.calls.load(Ordering::SeqCst), 0);
    }

    fn canonical_fixture_proposal() -> CanonicalPluginNodeOutcomeProposal {
        let output = serde_json::json!({"echo":42});
        let preserved_state = serde_json::json!({"cursor":1});
        let payload = serde_json::json!({"event_type":"user.plugin_ready"});
        let actions = vec![CanonicalPluginNodeActionProposal {
            kind: String::from("emit_event"),
            action_hash: plugin_node_action_hash("emit_event", &payload).expect("action"),
            payload,
        }];
        CanonicalPluginNodeOutcomeProposal {
            output_hash: plugin_node_value_hash(&output).expect("output"),
            output,
            preserved_state_hash: plugin_node_value_hash(&preserved_state).expect("state"),
            preserved_state,
            proposed_actions_hash: plugin_node_actions_hash(&actions).expect("actions"),
            proposed_actions: actions,
        }
    }

    fn production_fixture() -> (
        tempfile::TempDir,
        impl LocalRuntimeDataPort,
        PathBuf,
        DrivePluginTurnCommand,
    ) {
        let root = tempfile::tempdir().expect("root");
        let (mock, command) = fixture();
        let events = mock.inner.events.lock().expect("events").clone();
        let data =
            local_plugin_runtime_data(root.path().to_owned()).expect("local plugin runtime data");
        let session_directory = root.path().join(session_id().to_string());
        let persistence = SessionPersistenceLogic::new(data.clone());
        for event in events {
            persistence
                .commit_event(crate::persistence::CommitSessionEventCommand {
                    session_directory: session_directory.clone(),
                    event,
                    durability: CommitDurability::Full,
                })
                .expect("seed event");
        }
        (root, data, session_directory, command)
    }

    fn receipt_file(data: &impl FixtureFileDataPort, root: &std::path::Path) -> PathBuf {
        let directory = root
            .join(session_id().to_string())
            .join("artifacts")
            .join("plugin-node-receipts");
        data.list_fixture_directory(ListFixtureDirectoryDataRequest { directory })
            .expect("receipt directory")
            .into_iter()
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .expect("receipt file")
    }

    #[tokio::test]
    async fn approval_required_remains_proposed_and_never_dispatches() {
        let (journal, command) = fixture();
        let authorization = Arc::new(MockAuthorization::default());
        authorization.ask.store(true, Ordering::SeqCst);
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        let result = coordinator(
            journal.clone(),
            Arc::clone(&authorization),
            Arc::clone(&executor),
        )
        .drive(command)
        .await
        .expect("approval outcome");
        assert!(matches!(
            result.outcome,
            PluginTurnOutcome::AwaitingApproval { ref reason, .. }
                if reason == "approval_required"
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            &journal.event_types()[4..],
            ["plugin.node_invocation_proposed"]
        );
        let record = journal
            .load()
            .expect("head")
            .state
            .style_execution
            .expect("execution")
            .plugin_node_invocations
            .into_values()
            .next()
            .expect("invocation");
        assert_eq!(
            record.state,
            crate::session::PluginNodeInvocationState::Proposed
        );
    }

    #[tokio::test]
    async fn success_commits_exact_outbox_before_one_invoke_and_leaves_proposal_pending() {
        let (journal, command) = fixture();
        let authorization = Arc::new(MockAuthorization::default());
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        let result = coordinator(
            journal.clone(),
            Arc::clone(&authorization),
            Arc::clone(&executor),
        )
        .drive(command)
        .await
        .expect("plugin turn");
        assert!(matches!(
            result.outcome,
            PluginTurnOutcome::ProposalPendingValidation { .. }
        ));
        assert_eq!(authorization.calls.load(Ordering::SeqCst), 1);
        let observed = authorization.observed.lock().expect("observed");
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].declaration_hash,
            observed[0].identity.executor.executor_declaration_hash
        );
        drop(observed);
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            &journal.event_types()[4..],
            [
                "plugin.node_invocation_proposed",
                "plugin.node_invocation_authorized",
                "plugin.node_invocation_dispatched",
                "plugin.node_invocation_completed",
            ]
        );
        let state = journal.load().expect("head").state;
        let execution = state.style_execution.expect("execution");
        assert!(execution.completed_nodes.is_empty());
        assert!(execution.emitted_user_events.is_empty());
        assert!(
            execution
                .canonical_variables
                .expect("variables")
                .environment()
                .canonical_entries()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn isolated_executor_observes_exact_reserved_prior_state_projection() {
        let (_journal, mut command) = fixture();
        let prior = serde_json::json!({"cursor":"prior"});
        command.readable_state = merge_prior_plugin_state(
            command.readable_state,
            PriorPluginNodeState::Loaded {
                generation: 2,
                state_hash: plugin_node_value_hash(&prior).expect("prior hash"),
                state: prior.clone(),
            },
        )
        .expect("prior projection");
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        executor
            .execute_plugin_node(executor_command(&command).expect("executor command"))
            .await
            .expect("isolated plugin execution");
        let observed = executor.readable_states();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0]["$plugin_state"], prior);
        assert_eq!(observed[0]["classification"], "internal");
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the compact matrix proves both sides of every canonical append crash cut"
    )]
    async fn every_lifecycle_append_cut_recovers_without_duplicate_invocation() {
        let lifecycle = [
            "plugin.node_invocation_proposed",
            "plugin.node_invocation_authorized",
            "plugin.node_invocation_dispatched",
            "plugin.node_invocation_completed",
        ];
        for event_type in lifecycle {
            for after_append in [false, true] {
                let (journal, mut command) = fixture();
                if event_type == "plugin.node_invocation_dispatched" && after_append {
                    command.policy.idempotent = false;
                    command.policy.external_effects = true;
                }
                journal.fail_once(event_type, after_append);
                let authorization = Arc::new(MockAuthorization::default());
                let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
                let first = coordinator(
                    journal.clone(),
                    Arc::clone(&authorization),
                    Arc::clone(&executor),
                )
                .drive(command.clone())
                .await;
                assert!(matches!(first, Err(PluginTurnError::Journal(_))));

                let result = coordinator(
                    journal.clone(),
                    Arc::clone(&authorization),
                    Arc::clone(&executor),
                )
                .drive(command)
                .await
                .expect("restart");
                let dispatched_after_append =
                    event_type == "plugin.node_invocation_dispatched" && after_append;
                if dispatched_after_append {
                    assert!(matches!(
                        result.outcome,
                        PluginTurnOutcome::AmbiguousFailClosed { .. }
                    ));
                    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
                } else {
                    assert!(matches!(
                        result.outcome,
                        PluginTurnOutcome::ProposalPendingValidation { .. }
                    ));
                    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
                }
                for lifecycle_type in lifecycle {
                    assert!(journal.event_count(lifecycle_type) <= 1);
                }
            }
        }
    }

    fn disabled_plugin_events() -> Vec<RuntimeCommittedEvent> {
        let request_digest = ContentHash::digest(b"lifecycle-request");
        vec![
            RuntimeCommittedEvent::PluginLifecycleChangeRequested(
                crate::session::PluginLifecycleChangeRequestedEvent {
                    plugin_id: String::from("fixture.plugin-node"),
                    plugin_version: String::from("1.0.0"),
                    action: String::from("disable"),
                    reason_code: None,
                    request_digest,
                    cancellation_id: String::from("lifecycle-cancellation-1"),
                },
            ),
            RuntimeCommittedEvent::PluginLifecycleChanged(
                crate::session::PluginLifecycleChangedEvent {
                    plugin_id: String::from("fixture.plugin-node"),
                    plugin_version: String::from("1.0.0"),
                    state: String::from("disabled"),
                    reason_code: None,
                    request_digest,
                    host_audit_operation: String::from("disable"),
                    host_audit_outcome: String::from("disabled"),
                },
            ),
        ]
    }

    #[tokio::test]
    async fn concurrent_canonical_event_reloads_head_without_redispatch() {
        let (journal, command) = fixture();
        journal
            .inject_concurrent_events("plugin.node_invocation_ambiguous", disabled_plugin_events());
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Ambiguous));
        let result = coordinator(
            journal.clone(),
            Arc::new(MockAuthorization::default()),
            Arc::clone(&executor),
        )
        .drive(command)
        .await
        .expect("concurrent canonical append reconciliation");
        assert!(matches!(
            result.outcome,
            PluginTurnOutcome::AmbiguousFailClosed { .. }
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
        assert_eq!(journal.event_count("plugin.lifecycle_change_requested"), 1);
        assert_eq!(journal.event_count("plugin.lifecycle_changed"), 1);
        for event_type in [
            "plugin.node_invocation_proposed",
            "plugin.node_invocation_authorized",
            "plugin.node_invocation_dispatched",
            "plugin.node_invocation_ambiguous",
        ] {
            assert_eq!(journal.event_count(event_type), 1);
        }
    }

    #[tokio::test]
    async fn inactive_plugin_blocks_new_and_substituted_work_without_redispatch() {
        let (journal, command) = fixture();
        for event in disabled_plugin_events() {
            journal.commit_external_event(event);
        }
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        assert!(matches!(
            coordinator(
                journal.clone(),
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor),
            )
            .drive(command)
            .await,
            Err(PluginTurnError::Projection)
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert_eq!(journal.event_count("plugin.node_invocation_proposed"), 0);

        let (journal, command) = fixture();
        journal.fail_once("plugin.node_invocation_dispatched", true);
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        assert!(matches!(
            coordinator(
                journal.clone(),
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor),
            )
            .drive(command.clone())
            .await,
            Err(PluginTurnError::Journal(_))
        ));
        assert_eq!(journal.event_count("plugin.node_invocation_dispatched"), 1);
        for event in disabled_plugin_events() {
            journal.commit_external_event(event);
        }

        let mut changed_input = command.clone();
        changed_input.input = serde_json::json!({"value":"substituted"});
        assert!(matches!(
            coordinator(
                journal.clone(),
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor),
            )
            .drive(changed_input)
            .await,
            Err(PluginTurnError::Projection)
        ));

        let mut changed_executor = command;
        changed_executor.executor.executor_declaration_hash =
            ContentHash::digest(b"substituted-declaration");
        changed_executor.policy.declaration_hash =
            changed_executor.executor.executor_declaration_hash;
        assert!(matches!(
            coordinator(
                journal,
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor),
            )
            .drive(changed_executor)
            .await,
            Err(PluginTurnError::Projection)
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dispatched_restart_completes_only_from_exact_injected_receipt() {
        let (journal, mut command) = fixture();
        let identity = expected_identity(&journal, &command);
        journal.fail_once("plugin.node_invocation_dispatched", true);
        let authorization = Arc::new(MockAuthorization::default());
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        assert!(matches!(
            coordinator_with_scope(
                journal.clone(),
                Arc::clone(&authorization),
                Arc::clone(&executor),
                PersistenceStateScope::Session,
            )
            .drive(command.clone())
            .await,
            Err(PluginTurnError::Journal(_))
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        journal.inject_receipt(
            PluginNodeTerminalReceipt::seal(
                identity,
                PluginNodeTerminalReceiptOutcome::Completed {
                    proposal: Box::new(canonical_fixture_proposal()),
                    attempts: 1,
                },
            )
            .expect("receipt"),
        );
        command.readable_state = serde_json::json!({
            "classification": "graph-vars-only-after-state-store-unavailable"
        });
        let result = coordinator_with_scope(
            journal,
            Arc::clone(&authorization),
            Arc::clone(&executor),
            PersistenceStateScope::Session,
        )
        .drive(command)
        .await
        .expect("receipt recovery");
        assert!(matches!(
            result.outcome,
            PluginTurnOutcome::ProposalPendingValidation { .. }
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invalid_timeout_unavailable_and_ambiguous_outcomes_fail_closed() {
        for (behavior, risky, ambiguous) in [
            (ExecutorBehavior::InvalidAction, false, false),
            (ExecutorBehavior::Unavailable, false, false),
            (ExecutorBehavior::Timeout, false, false),
            (ExecutorBehavior::Unavailable, true, true),
            (ExecutorBehavior::Ambiguous, false, true),
        ] {
            let (journal, mut command) = fixture();
            if risky {
                command.policy.idempotent = false;
                command.policy.external_effects = true;
            }
            let executor = Arc::new(MockExecutor::new(behavior));
            let result = coordinator(
                journal,
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor),
            )
            .drive(command)
            .await
            .expect("terminal classification");
            assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
            if ambiguous {
                assert!(matches!(
                    result.outcome,
                    PluginTurnOutcome::AmbiguousFailClosed { .. }
                ));
            } else {
                assert!(matches!(result.outcome, PluginTurnOutcome::Failed { .. }));
            }
        }
    }

    #[tokio::test]
    async fn cancellation_and_exact_command_drift_never_cross_plugin_boundary() {
        let (journal, command) = fixture();
        journal.cancel();
        let authorization = Arc::new(MockAuthorization::default());
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        let result = coordinator(
            journal.clone(),
            Arc::clone(&authorization),
            Arc::clone(&executor),
        )
        .drive(command.clone())
        .await
        .expect("cancelled");
        assert!(matches!(result.outcome, PluginTurnOutcome::Failed { .. }));
        assert_eq!(authorization.calls.load(Ordering::SeqCst), 0);
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert_eq!(journal.event_count("plugin.node_invocation_dispatched"), 0);

        let (journal, mut command) = fixture();
        command.policy.declaration_hash = ContentHash::digest(b"substituted declaration");
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        assert!(matches!(
            coordinator(
                journal,
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor)
            )
            .drive(command)
            .await,
            Err(PluginTurnError::Projection)
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn tampered_receipt_and_changed_restart_input_are_rejected_without_redispatch() {
        let (journal, command) = fixture();
        let identity = expected_identity(&journal, &command);
        journal.fail_once("plugin.node_invocation_dispatched", true);
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        assert!(
            coordinator(
                journal.clone(),
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor)
            )
            .drive(command.clone())
            .await
            .is_err()
        );

        let mut receipt = PluginNodeTerminalReceipt::seal(
            identity.clone(),
            PluginNodeTerminalReceiptOutcome::Completed {
                proposal: Box::new(canonical_fixture_proposal()),
                attempts: 1,
            },
        )
        .expect("receipt");
        receipt.receipt_hash = ContentHash::digest(b"tampered");
        journal.inject_receipt(receipt);
        assert!(matches!(
            coordinator(
                journal.clone(),
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor)
            )
            .drive(command.clone())
            .await,
            Err(PluginTurnError::InvalidReceipt)
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);

        let mut substituted_identity = identity;
        substituted_identity.readable_state_hash =
            ContentHash::digest(b"substituted-readable-state");
        journal.inject_receipt(
            PluginNodeTerminalReceipt::seal(
                substituted_identity,
                PluginNodeTerminalReceiptOutcome::Completed {
                    proposal: Box::new(canonical_fixture_proposal()),
                    attempts: 1,
                },
            )
            .expect("self-consistent substituted receipt"),
        );
        assert!(matches!(
            coordinator(
                journal.clone(),
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor)
            )
            .drive(command.clone())
            .await,
            Err(PluginTurnError::InvalidReceipt)
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);

        let mut changed = command;
        changed.input = serde_json::json!({"value":43});
        assert!(
            coordinator(
                journal,
                Arc::new(MockAuthorization::default()),
                Arc::clone(&executor)
            )
            .drive(changed)
            .await
            .is_err()
        );
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the filesystem integration retains restart, CAS, corruption, and cancellation evidence in one exact session fixture"
    )]
    async fn production_adapter_uses_real_journal_receipts_cas_and_cancellation() {
        let (root, data, session_directory, command) = production_fixture();
        let journal = SessionPluginTurnJournal::new(
            data.clone(),
            command.session_id,
            session_directory.clone(),
        );
        let initial = journal.load().expect("initial head");
        let identity = expected_identity_from_head(&initial, &command);
        let receipt = PluginNodeTerminalReceipt::seal(
            identity.clone(),
            PluginNodeTerminalReceiptOutcome::Completed {
                proposal: Box::new(canonical_fixture_proposal()),
                attempts: 1,
            },
        )
        .expect("receipt");
        journal
            .store_terminal_receipt(receipt.clone())
            .expect("store");
        journal
            .store_terminal_receipt(receipt.clone())
            .expect("exact duplicate");
        let restarted = SessionPluginTurnJournal::new(
            data.clone(),
            command.session_id,
            session_directory.clone(),
        );
        assert_eq!(
            restarted
                .terminal_receipt(&identity.invocation_id)
                .expect("restart load"),
            Some(receipt.clone())
        );

        let substituted = PluginNodeTerminalReceipt::seal(
            identity.clone(),
            PluginNodeTerminalReceiptOutcome::Failed {
                code: String::from("substituted"),
                diagnostic: None,
                attempts: 1,
            },
        )
        .expect("substituted receipt");
        assert_eq!(
            restarted
                .store_terminal_receipt(substituted)
                .expect_err("conflict")
                .code,
            "receipt_conflict"
        );

        let stale_identity = restarted.allocate_identity().expect("event identity");
        let stale_event = seal_event(
            &initial,
            initial
                .state
                .last_sequence
                .checked_next()
                .expect("sequence"),
            stale_identity,
            RuntimeCommittedEvent::PluginNodeInvocationProposed(Box::new(
                PluginNodeInvocationProposedEvent {
                    identity: identity.clone(),
                },
            )),
        )
        .expect("stale event");
        let result = PluginTurnCoordinator::new(
            restarted.clone(),
            Arc::new(MockAuthorization::default()),
            Arc::new(MockExecutor::new(ExecutorBehavior::Success)),
        )
        .drive(command.clone())
        .await
        .expect("real coordinator");
        assert!(matches!(
            result.outcome,
            PluginTurnOutcome::ProposalPendingValidation { .. }
        ));
        assert_eq!(
            restarted
                .append(
                    PluginTurnAppendPosition {
                        sequence: initial.state.last_sequence,
                        event_id: initial.last_event_id,
                    },
                    stale_event,
                )
                .expect_err("stale head")
                .code,
            "append_conflict"
        );

        let file = receipt_file(&data, root.path());
        data.corrupt_fixture_file(CorruptFixtureFileDataRequest { file })
            .expect("corrupt receipt");
        assert_eq!(
            restarted
                .terminal_receipt(&identity.invocation_id)
                .expect_err("corrupt")
                .code,
            "receipt_corrupt"
        );

        let (_root, data, session_directory, command) = production_fixture();
        data.request_runtime_cancellation(RequestRuntimeCancellationDataCommand {
            cancellation_id: command.cancellation_id.clone(),
        })
        .expect("request cancellation");
        let executor = Arc::new(MockExecutor::new(ExecutorBehavior::Success));
        let cancelled = PluginTurnCoordinator::new(
            SessionPluginTurnJournal::new(data, command.session_id, session_directory),
            Arc::new(MockAuthorization::default()),
            Arc::clone(&executor),
        )
        .drive(command)
        .await
        .expect("cancelled");
        assert!(matches!(
            cancelled.outcome,
            PluginTurnOutcome::Failed { .. }
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    fn expected_identity_from_head(
        head: &PluginTurnHead,
        command: &DrivePluginTurnCommand,
    ) -> PluginNodeInvocationIdentity {
        let plugin_command = executor_command(command).expect("plugin command");
        let (invocation_id, invocation_digest) =
            plugin_node_invocation_identity(&plugin_command).expect("identity");
        let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
            panic!("plugin source")
        };
        PluginNodeInvocationIdentity {
            work: command.work.clone(),
            executor: command.executor.clone(),
            configuration_hash: command.executor.adapter_configuration_reference,
            plugin_id: plugin_id.clone(),
            invocation_id,
            invocation_digest,
            input_hash: plugin_node_value_hash(&command.input).expect("input"),
            readable_state_hash: plugin_node_value_hash(&command.readable_state).expect("state"),
            causation_event_id: head.last_event_id,
        }
    }
}
