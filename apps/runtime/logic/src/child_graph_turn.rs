//! Durable, style-independent coordination for child graph nodes.
//!
//! This module is the runtime-logic seam between the pure child graph
//! coordinator/application planner and a later `TurnLogic` adapter. It owns
//! canonical journal ordering while leaving policy, child-session creation,
//! reconciliation, observation, and cancellation behind a bounded effect
//! port. No effect implementation can append events or mutate graph state.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str::FromStr,
};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{
    CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId, TimestampMillis, Version,
};
use agentmod_runtime_data::{
    cancellation::{RequestRuntimeCancellationDataCommand, RuntimeCancellationControlDataPort},
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    journal::JournalEventDataPort,
    node_executor::NodeExecutorDataPort,
    registry::{ListSessionsDataRequest, SessionRegistryDataPort},
    style::SessionStyleDataPort,
    workspace::WorkspaceLeaseDataPort,
};
use agentmod_session_style_sdk::ChildMemoryAccess;
use async_trait::async_trait;
use thiserror::Error;

use crate::{
    child_graph_ancillary_policy::{
        ChildGraphReviewerCommand, ChildGraphReviewerOutcome, ChildGraphReviewerUseCasePort,
        InterceptionChildGraphConsequentialPolicy, RuntimeChildGraphAncillaryApplication,
        reviewer_result_hash,
    },
    child_graph_application::{
        ChildCreationEvidence, ChildGraphApplicationError, ChildGraphApplicationEvidence,
        ChildTerminalEvidence, PlanChildGraphApplicationCommand, plan_child_graph_application,
    },
    child_graph_continuation::ContinuationChildGraphAncillaryEffects,
    child_graph_execution::{
        ChildGraphNodeOutcome, ChildSpawnProposal, ChildWaitProjection, ReviewRoutingProposal,
    },
    child_session::{ChildSessionLogicPort, EnsureChildSessionCommand, RuntimeChildSessionLogic},
    continuation::ContinuationLogic,
    harness::ProviderExecutionPolicy,
    node_execution::NodeWorkIdentity,
    persistence::{
        CommitDurability, CompareAppendSessionEventCommand, CompareAppendSessionEventResult,
        LoadSessionCommand, SessionPersistenceLogic, SessionPersistenceLogicPort,
    },
    session::{
        ChildAgentRecord, ChildAgentState, GenericChildCancellationAmbiguousEvent,
        GenericChildCancellationAuthorizedEvent, GenericChildCancellationChildReceipt,
        GenericChildCancellationCompletedEvent, GenericChildCancellationDispatchedEvent,
        GenericChildCancellationIdentity, GenericChildCancellationReceipt,
        GenericChildCancellationRecord, GenericChildCancellationRequestedEvent,
        GenericChildCancellationState, GenericChildExecutionIdentity, GenericChildSpawnContract,
        GenericChildTerminalDisposition, GenericChildTerminalReceipt, RuntimeCommittedEvent,
        SessionLifecycle, SessionLifecycleChangedEvent, SessionNodeExecutorResolution,
        SessionPermissionDefaults, SessionReducerError, SessionState, generic_child_action_digest,
        generic_child_cancellation_dispatch_hash, generic_child_cancellation_identity_hash,
        generic_child_cancellation_receipt_hash, generic_child_dispatch_hash,
        generic_child_link_hash, generic_child_terminal_receipt_hash, reduce,
    },
    style::StyleEnvironment,
    workspace::{WorkspaceLeaseMode, WorkspaceMergePolicy},
};

const MAX_APPEND_RETRIES: usize = 32;
const MAX_CHILDREN_PER_NODE: u32 = 1_024;
const MAX_REFERENCE_BYTES: usize = 1_024;

/// Exact canonical replay cut used to plan one append.
#[derive(Clone, Debug)]
pub struct ChildGraphTurnHead {
    /// Pure session projection reconstructed from the committed journal.
    pub state: SessionState,
    /// Identity of the exact journal head.
    pub last_event_id: EventId,
}

/// Runtime-owned identity allocated for one canonical child event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildGraphTurnEventIdentity {
    /// Unique committed-event identity.
    pub event_id: EventId,
    /// Runtime-recorded event time.
    pub timestamp: TimestampMillis,
    /// Stable session/run correlation identity.
    pub correlation_id: CorrelationId,
}

/// Expected canonical head supplied to compare-and-swap append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildGraphTurnAppendPosition {
    /// Sequence at the loaded head.
    pub sequence: Sequence,
    /// Event identity at the loaded head.
    pub event_id: EventId,
}

/// Result of a head-bound journal append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildGraphTurnAppendOutcome {
    /// The sealed event was durably appended.
    Appended,
    /// Another writer changed the head; replay and planning must restart.
    Conflict,
}

/// Stable journal-boundary failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("child graph turn journal failed: {code}")]
pub struct ChildGraphTurnJournalError {
    /// Bounded diagnostic code.
    pub code: String,
}

/// Runtime-logic-owned canonical journal boundary.
pub trait ChildGraphTurnJournal: Send + Sync + 'static {
    /// Loads and purely replays the exact current session head.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when load or replay fails.
    fn load(&self) -> Result<ChildGraphTurnHead, ChildGraphTurnJournalError>;

    /// Allocates runtime-owned event identity and time.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error when allocation fails.
    fn allocate_identity(&self) -> Result<ChildGraphTurnEventIdentity, ChildGraphTurnJournalError>;

    /// CAS-appends one sealed event previously accepted by the reducer.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error for durable storage failure.
    fn append(
        &self,
        expected: ChildGraphTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<ChildGraphTurnAppendOutcome, ChildGraphTurnJournalError>;
}

/// Production data-backed journal adapter for child graph coordination.
#[derive(Clone, Debug)]
pub struct SessionChildGraphTurnJournal<D> {
    data: D,
    persistence: SessionPersistenceLogic<D>,
    session_id: SessionId,
    session_directory: PathBuf,
}

impl<D> SessionChildGraphTurnJournal<D>
where
    D: Clone,
{
    /// Binds one exact canonical session journal.
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

impl<D> ChildGraphTurnJournal for SessionChildGraphTurnJournal<D>
where
    D: Clone + Send + Sync + EventIdentityDataPort + JournalEventDataPort + 'static,
{
    fn load(&self) -> Result<ChildGraphTurnHead, ChildGraphTurnJournalError> {
        self.persistence
            .load_session(LoadSessionCommand {
                session_directory: self.session_directory.clone(),
                expected_session_id: self.session_id,
            })
            .map(|loaded| ChildGraphTurnHead {
                state: loaded.state,
                last_event_id: loaded.last_event_id,
            })
            .map_err(|_| child_graph_journal_error("load_failed"))
    }

    fn allocate_identity(&self) -> Result<ChildGraphTurnEventIdentity, ChildGraphTurnJournalError> {
        self.data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map(|identity| ChildGraphTurnEventIdentity {
                event_id: identity.event_id,
                timestamp: identity.timestamp,
                correlation_id: identity.correlation_id,
            })
            .map_err(|_| child_graph_journal_error("identity_unavailable"))
    }

    fn append(
        &self,
        expected: ChildGraphTurnAppendPosition,
        event: EventEnvelope<RuntimeCommittedEvent>,
    ) -> Result<ChildGraphTurnAppendOutcome, ChildGraphTurnJournalError> {
        let expected_sequence = expected
            .sequence
            .checked_next()
            .map_err(|_| child_graph_journal_error("append_sequence_overflow"))?;
        if event.metadata.sequence != expected_sequence
            || event.metadata.scope != EventScope::Session(self.session_id)
        {
            return Err(child_graph_journal_error("append_identity_mismatch"));
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
            .map_err(|_| child_graph_journal_error("append_failed"))?
        {
            CompareAppendSessionEventResult::Appended(committed)
                if committed.event_id == event_id && committed.sequence == sequence =>
            {
                Ok(ChildGraphTurnAppendOutcome::Appended)
            }
            CompareAppendSessionEventResult::Appended(_) => {
                Err(child_graph_journal_error("append_receipt_mismatch"))
            }
            CompareAppendSessionEventResult::Conflict => Ok(ChildGraphTurnAppendOutcome::Conflict),
        }
    }
}

fn child_graph_journal_error(code: &'static str) -> ChildGraphTurnJournalError {
    ChildGraphTurnJournalError {
        code: code.to_owned(),
    }
}

/// Exact policy request for one canonically proposed child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildCreationAuthorizationRequest {
    /// Exact generic child identity.
    pub identity: GenericChildExecutionIdentity,
    /// Exact bounded immutable creation contract.
    pub contract: GenericChildSpawnContract,
    /// Selected child style from the immutable proposal.
    pub child_style: String,
    /// Hard child token budget.
    pub token_budget: u64,
    /// Digest of the consequential action requiring authorization.
    pub action_digest: ContentHash,
    /// Sequence at which the proposal became canonical.
    pub proposed_at: Sequence,
}

/// Typed policy result for child creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildCreationAuthorizationOutcome {
    /// Exact action digest was approved.
    Approved {
        /// Approved digest; substitutions fail closed.
        action_digest: ContentHash,
    },
    /// Approval remains pending in the normal continuation system.
    Waiting {
        /// Opaque reference owned and persisted by the later Turn adapter.
        continuation_reference: String,
    },
    /// Policy terminally denied the action.
    Denied {
        /// Stable redacted denial code.
        code: String,
    },
}

/// Exact creation outbox request, valid only after canonical dispatch intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildCreationDispatchRequest {
    /// Exact generic child identity.
    pub identity: GenericChildExecutionIdentity,
    /// Exact bounded immutable creation contract.
    pub contract: GenericChildSpawnContract,
    /// Selected child style.
    pub child_style: String,
    /// Hard token budget.
    pub token_budget: u64,
    /// Approved action digest.
    pub action_digest: ContentHash,
    /// Hash of the exact dispatch intent.
    pub dispatch_hash: ContentHash,
    /// Parent proposal sequence retained by the child link.
    pub parent_action_sequence: Sequence,
    /// Sequence at which dispatch intent became canonical.
    pub dispatched_at: Sequence,
}

/// Definite or uncertain result of child creation/reconciliation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildCreationEffectOutcome {
    /// Exact atomic child-creation receipt.
    Created(Box<ChildCreationEvidence>),
    /// Boundary has not produced a terminal receipt.
    Waiting {
        /// Stable bounded diagnostic code.
        code: String,
    },
    /// The effect may have happened; automatic creation is prohibited.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
    },
    /// The boundary proved that creation did not succeed.
    Failed {
        /// Stable bounded failure code.
        code: String,
    },
}

/// Exact request to observe one canonically active child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildTerminalObservationRequest {
    /// Exact generic child identity.
    pub identity: GenericChildExecutionIdentity,
    /// Runtime-managed child session.
    pub child_session_id: SessionId,
    /// Exact immutable parent/child link hash.
    pub child_link_hash: ContentHash,
}

/// Result of observing an active child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildTerminalObservationOutcome {
    /// Child remains non-terminal.
    Pending,
    /// Exact verified terminal evidence.
    Terminal(ChildTerminalEvidence),
    /// Observation cannot exclude a substituted or partial terminal effect.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
    },
}

/// Canonically evidenced request to propose cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildCancellationProposalRequest {
    /// Parent session owning the graph.
    pub session_id: SessionId,
    /// Exact wait-node work.
    pub work: NodeWorkIdentity,
    /// Immutable execution plan owning the wait.
    pub execution_plan_hash: ContentHash,
    /// Exact compiled wait-node configuration.
    pub configuration_hash: ContentHash,
    /// Hash of the committed wait projection.
    pub projection_hash: ContentHash,
    /// Stable wait failure code.
    pub reason: String,
    /// Stable exact child set targeted by the compiled wait policy.
    pub child_ids: Vec<SessionId>,
}

/// Typed cancellation-proposal result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildCancellationProposalOutcome {
    /// Normal policy/effect handling accepted the proposal.
    Proposed {
        /// Opaque bounded proposal reference.
        proposal_reference: String,
    },
    /// Proposal awaits a normal approval continuation.
    Waiting {
        /// Opaque continuation reference.
        continuation_reference: String,
    },
    /// Cancellation proposal was denied.
    Denied {
        /// Stable redacted denial code.
        code: String,
    },
    /// Cancellation handling is ambiguous and must fail closed.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
    },
}

/// Exact approved cancellation request supplied only after parent dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildCancellationDispatchRequest {
    /// Complete immutable cancellation identity.
    pub identity: GenericChildCancellationIdentity,
    /// Exact policy-approved action digest.
    pub action_digest: ContentHash,
    /// Parent-journal outbox digest.
    pub dispatch_hash: ContentHash,
}

/// Typed child-session cancellation boundary result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildCancellationEffectOutcome {
    /// Every exact owned child is canonically cancelled.
    Completed {
        /// Stable sorted exact child heads.
        children: Vec<GenericChildCancellationChildReceipt>,
    },
    /// The effect may be missing or partial; callers must never redispatch.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
    },
}

/// Exact review evidence verification request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildReviewEvidenceRequest {
    /// Parent session owning the graph.
    pub session_id: SessionId,
    /// Exact review-node work.
    pub work: NodeWorkIdentity,
    /// Immutable plan hash.
    pub execution_plan_hash: ContentHash,
    /// Immutable compiled-node hash.
    pub configuration_hash: ContentHash,
    /// Runtime-validated pure review routing proposal.
    pub routing: ReviewRoutingProposal,
}

/// Typed review evidence verification result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildReviewEvidenceOutcome {
    /// Exact routing evidence is verified.
    Validated {
        /// Hash must equal the pure routing proposal evidence.
        evidence_hash: ContentHash,
    },
    /// Evidence is not yet available.
    Waiting {
        /// Opaque bounded wait reference.
        continuation_reference: String,
    },
    /// Evidence is terminally invalid.
    Rejected {
        /// Stable redacted rejection code.
        code: String,
    },
    /// Evidence may refer to an unclassified external effect.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
    },
}

/// Runtime-logic-owned effect boundary for child graph execution.
#[async_trait]
pub trait ChildGraphEffectPort: Send + Sync + 'static {
    /// Applies normal proposal/interceptor/user/mandatory policy.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error; no journal mutation is permitted.
    async fn authorize_creation(
        &self,
        request: ChildCreationAuthorizationRequest,
    ) -> Result<ChildCreationAuthorizationOutcome, ChildGraphEffectError>;

    /// Creates a child exactly once after a dispatch appended by this call.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error; callers classify it as ambiguous.
    async fn create_after_dispatch(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError>;

    /// Reconciles a pre-existing dispatch intent without redispatching.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error; callers fail closed.
    async fn reconcile_creation(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError>;

    /// Observes a child through the authoritative child-session boundary.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error without changing canonical state.
    async fn observe_terminal(
        &self,
        request: ChildTerminalObservationRequest,
    ) -> Result<ChildTerminalObservationOutcome, ChildGraphEffectError>;

    /// Proposes cancellation after the wait failure is canonical.
    ///
    /// This method does not authorize implementations to kill a child
    /// directly; the production adapter must use the normal consequential
    /// action path and return only its typed proposal outcome.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error without changing canonical state.
    async fn propose_cancellation(
        &self,
        request: ChildCancellationProposalRequest,
    ) -> Result<ChildCancellationProposalOutcome, ChildGraphEffectError>;

    /// Dispatches a freshly committed exact cancellation outbox once.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error; callers commit ambiguity and never
    /// redispatch the request.
    async fn cancel_after_dispatch(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError>;

    /// Reconciles a pre-existing cancellation outbox without redispatch.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error; callers fail closed.
    async fn reconcile_cancellation(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError>;

    /// Verifies exact review evidence before canonical routing.
    ///
    /// # Errors
    ///
    /// Returns a stable boundary error without changing canonical state.
    async fn validate_review_evidence(
        &self,
        request: ChildReviewEvidenceRequest,
    ) -> Result<ChildReviewEvidenceOutcome, ChildGraphEffectError>;
}

/// Child-session-only production boundary used by the composite effect port.
pub trait ChildGraphChildSessionPort: Send + Sync + 'static {
    /// Creates or exactly recovers a child after a freshly appended dispatch.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without committing parent events.
    fn create_after_dispatch(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError>;

    /// Reconciles a replayed dispatch without creating a missing child.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without committing parent events.
    fn reconcile_creation(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError>;

    /// Observes exact child terminal state from its canonical journal.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without committing parent events.
    fn observe_terminal(
        &self,
        request: ChildTerminalObservationRequest,
    ) -> Result<ChildTerminalObservationOutcome, ChildGraphEffectError>;

    /// Cancels an exact owned child set after parent dispatch.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without committing parent events.
    fn cancel_after_dispatch(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError>;

    /// Reconciles an existing exact cancellation receipt without redispatch.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without committing parent events.
    fn reconcile_cancellation(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError>;
}

/// Consequential-policy and reviewer adapter supplied by the future Turn seam.
///
/// This deliberately excludes child creation and observation so a policy
/// adapter cannot fabricate child receipts.
#[async_trait]
pub trait ChildGraphAncillaryEffectPort: Send + Sync + 'static {
    /// Applies the normal consequential authorization pipeline.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without changing graph state.
    async fn authorize_creation(
        &self,
        request: ChildCreationAuthorizationRequest,
    ) -> Result<ChildCreationAuthorizationOutcome, ChildGraphEffectError>;

    /// Proposes cancellation through the normal consequential action path.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without changing graph state.
    async fn propose_cancellation(
        &self,
        request: ChildCancellationProposalRequest,
    ) -> Result<ChildCancellationProposalOutcome, ChildGraphEffectError>;

    /// Validates reviewer evidence through the selected provider/plugin path.
    ///
    /// # Errors
    ///
    /// Returns a stable logic error without changing graph state.
    async fn validate_review_evidence(
        &self,
        request: ChildReviewEvidenceRequest,
    ) -> Result<ChildReviewEvidenceOutcome, ChildGraphEffectError>;
}

/// Concrete composite effect adapter used by the production Turn composition.
pub struct ProductionChildGraphEffectPort<C, A> {
    children: C,
    ancillary: A,
}

/// Complete runtime context required to execute one exact child graph node.
#[derive(Clone)]
pub struct ExecuteProductionChildGraphTurnCommand {
    /// Canonical child graph command produced by the pure coordinator.
    pub node: CoordinateChildGraphTurnCommand,
    /// Root containing every runtime-owned session.
    pub sessions_root: PathBuf,
    /// Exact parent session directory.
    pub session_directory: PathBuf,
    /// Parent workspace retained by canonical replay.
    pub workspace: String,
    /// Parent style retained by canonical replay.
    pub style: String,
    /// Immutable memory delegation policy.
    pub memory_access: ChildMemoryAccess,
    /// Exact composed policy for this session and invocation.
    pub policy: ProviderExecutionPolicy,
    /// Immutable style-owned approval defaults bound at session creation.
    pub permission_defaults: SessionPermissionDefaults,
    /// Immutable style-owned tool groups used to classify approval overrides.
    pub allowed_tool_groups: Vec<String>,
    /// Canonical provider-backed reviewer receipt for a review node.
    pub reviewer_receipt: Option<CanonicalChildGraphReviewerReceipt>,
}

/// Exact canonical provider result accepted for one generic review node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalChildGraphReviewerReceipt {
    /// Exact graph work that invoked the reviewer.
    pub work: NodeWorkIdentity,
    /// Immutable execution plan owning the review.
    pub execution_plan_hash: ContentHash,
    /// Exact compiled review-node configuration hash.
    pub configuration_hash: ContentHash,
    /// Runtime-validated pure routing result.
    pub routing: ReviewRoutingProposal,
    /// Hash of the terminal canonical provider receipt and visible payload.
    pub provider_result_hash: ContentHash,
}

/// Runtime-owned generic child-node execution boundary used by `TurnLogic`.
#[async_trait]
pub trait ChildGraphNodeTurnPort: Send + Sync + 'static {
    /// Executes or recovers the exact persisted child executor.
    ///
    /// # Errors
    ///
    /// Fails closed when canonical replay, policy, continuation, child-session,
    /// or reviewer validation cannot prove an exact result.
    async fn execute(
        &self,
        command: ExecuteProductionChildGraphTurnCommand,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError>;
}

/// Production generic child-node execution boundary.
#[derive(Clone)]
pub struct ProductionChildGraphNodeTurnPort<D> {
    data: D,
    environment: StyleEnvironment,
}

impl<D> ProductionChildGraphNodeTurnPort<D> {
    /// Binds runtime data and the immutable child style discovery environment.
    #[must_use]
    pub const fn new(data: D, environment: StyleEnvironment) -> Self {
        Self { data, environment }
    }
}

/// Fail-closed verifier for the terminal provider receipt obtained through
/// the runtime-owned review use case before child-graph application.
#[derive(Clone, Debug, Default)]
struct CanonicalReceiptChildGraphReviewer {
    receipt: Option<CanonicalChildGraphReviewerReceipt>,
}

#[async_trait]
impl ChildGraphReviewerUseCasePort for CanonicalReceiptChildGraphReviewer {
    async fn review(
        &self,
        command: ChildGraphReviewerCommand,
    ) -> Result<
        ChildGraphReviewerOutcome,
        crate::child_graph_continuation::ChildGraphAncillaryApplicationError,
    > {
        let Some(receipt) = self.receipt.as_ref() else {
            return Ok(ChildGraphReviewerOutcome::Denied {
                code: String::from("child_graph_reviewer_receipt_missing"),
            });
        };
        if receipt.work != command.request.work
            || receipt.execution_plan_hash != command.request.execution_plan_hash
            || receipt.configuration_hash != command.request.configuration_hash
            || receipt.routing != command.request.routing
            || receipt.provider_result_hash == ContentHash::from_bytes([0; 32])
        {
            return Ok(ChildGraphReviewerOutcome::Denied {
                code: String::from("child_graph_reviewer_receipt_substitution"),
            });
        }
        Ok(ChildGraphReviewerOutcome::Completed {
            routing: receipt.routing.clone(),
            request_hash: command.request_hash,
            result_hash: reviewer_result_hash(command.request_hash, &receipt.routing)?,
        })
    }
}

#[async_trait]
impl<D> ChildGraphNodeTurnPort for ProductionChildGraphNodeTurnPort<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + NodeExecutorDataPort
        + RuntimeCancellationControlDataPort
        + SessionRegistryDataPort
        + SessionStyleDataPort
        + WorkspaceLeaseDataPort
        + agentmod_runtime_data::continuation::ContinuationDataPort
        + 'static,
{
    async fn execute(
        &self,
        command: ExecuteProductionChildGraphTurnCommand,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        let journal = SessionChildGraphTurnJournal::new(
            self.data.clone(),
            command.node.session_id,
            command.session_directory.clone(),
        );
        let children = RuntimeChildGraphChildSessions::new(
            self.data.clone(),
            self.environment.clone(),
            command.sessions_root,
            command.node.session_id,
            command.session_directory,
            command.workspace.clone(),
            command.memory_access,
        );
        let policy = InterceptionChildGraphConsequentialPolicy::with_style_defaults(
            command.policy,
            &command.permission_defaults,
            &command.allowed_tool_groups,
        )
        .map_err(|_| ChildGraphTurnError::InvalidCommand)?;
        let application = RuntimeChildGraphAncillaryApplication::new(
            policy,
            CanonicalReceiptChildGraphReviewer {
                receipt: command.reviewer_receipt,
            },
            command.style,
            command.workspace,
        );
        let ancillary = ContinuationChildGraphAncillaryEffects::new(
            ContinuationLogic::new(self.data.clone()),
            application,
        );
        ChildGraphTurnCoordinator::new(
            journal,
            ProductionChildGraphEffectPort::new(children, ancillary),
        )
        .coordinate(&command.node)
        .await
    }
}

impl<C, A> ProductionChildGraphEffectPort<C, A> {
    /// Composes real child-session effects with the Turn-owned policy seam.
    #[must_use]
    pub const fn new(children: C, ancillary: A) -> Self {
        Self {
            children,
            ancillary,
        }
    }
}

#[async_trait]
impl<C, A> ChildGraphEffectPort for ProductionChildGraphEffectPort<C, A>
where
    C: ChildGraphChildSessionPort,
    A: ChildGraphAncillaryEffectPort,
{
    async fn authorize_creation(
        &self,
        request: ChildCreationAuthorizationRequest,
    ) -> Result<ChildCreationAuthorizationOutcome, ChildGraphEffectError> {
        self.ancillary.authorize_creation(request).await
    }

    async fn create_after_dispatch(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError> {
        self.children.create_after_dispatch(request)
    }

    async fn reconcile_creation(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError> {
        self.children.reconcile_creation(request)
    }

    async fn observe_terminal(
        &self,
        request: ChildTerminalObservationRequest,
    ) -> Result<ChildTerminalObservationOutcome, ChildGraphEffectError> {
        self.children.observe_terminal(request)
    }

    async fn propose_cancellation(
        &self,
        request: ChildCancellationProposalRequest,
    ) -> Result<ChildCancellationProposalOutcome, ChildGraphEffectError> {
        self.ancillary.propose_cancellation(request).await
    }

    async fn cancel_after_dispatch(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError> {
        self.children.cancel_after_dispatch(request)
    }

    async fn reconcile_cancellation(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError> {
        self.children.reconcile_cancellation(request)
    }

    async fn validate_review_evidence(
        &self,
        request: ChildReviewEvidenceRequest,
    ) -> Result<ChildReviewEvidenceOutcome, ChildGraphEffectError> {
        self.ancillary.validate_review_evidence(request).await
    }
}

/// Real child-session adapter over runtime logic and runtime data ports.
#[derive(Clone)]
pub struct RuntimeChildGraphChildSessions<D> {
    data: D,
    child_logic: RuntimeChildSessionLogic<D>,
    sessions_root: PathBuf,
    parent_session_id: SessionId,
    parent_session_directory: PathBuf,
    workspace: String,
    memory_access: ChildMemoryAccess,
}

impl<D> RuntimeChildGraphChildSessions<D>
where
    D: Clone,
{
    /// Binds exact parent storage, workspace, style environment, and memory
    /// policy for generic child nodes.
    #[must_use]
    pub fn new(
        data: D,
        environment: StyleEnvironment,
        sessions_root: PathBuf,
        parent_session_id: SessionId,
        parent_session_directory: PathBuf,
        workspace: String,
        memory_access: ChildMemoryAccess,
    ) -> Self {
        Self {
            child_logic: RuntimeChildSessionLogic::new(data.clone(), environment),
            data,
            sessions_root,
            parent_session_id,
            parent_session_directory,
            workspace,
            memory_access,
        }
    }
}

impl<D> ChildGraphChildSessionPort for RuntimeChildGraphChildSessions<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + NodeExecutorDataPort
        + RuntimeCancellationControlDataPort
        + SessionRegistryDataPort
        + SessionStyleDataPort
        + WorkspaceLeaseDataPort
        + 'static,
{
    fn create_after_dispatch(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError> {
        let validated = validate_child_dispatch_request(&request, self.parent_session_id)?;
        let result = self
            .child_logic
            .clone()
            .with_maximum_cost_budget_micros(request.contract.cost_budget_micros)
            .with_workspace_mode(validated.workspace_mode)
            .ensure_child_session(EnsureChildSessionCommand {
                sessions_root: self.sessions_root.clone(),
                parent_session_id: self.parent_session_id,
                parent_action_sequence: request.parent_action_sequence,
                parent_graph_node_id: request.identity.work.node_id.clone(),
                workspace: self.workspace.clone(),
                style_selector: request.child_style.clone(),
                inherited_provider: request.contract.inherited_provider.clone(),
                inherited_model: request.contract.inherited_model.clone(),
                inherited_mcp: request.contract.inherited_mcp.clone(),
                artifact_references: request.contract.artifact_references.clone(),
                task_id: request.identity.task_id.clone(),
                revision: request.identity.work.loop_iteration,
                depth: request.contract.depth,
                task: validated.task,
                token_budget: request.token_budget,
                context_budget_tokens: request.contract.context_budget_tokens,
                tool_groups: request.contract.tool_groups.iter().cloned().collect(),
                memory_access: self.memory_access,
            })
            .map_err(|error| child_graph_effect_error(&format!("child_create:{error}")))?;
        if result.parent_action_sequence != request.parent_action_sequence {
            return Err(child_graph_effect_error(
                "child_create_receipt_substitution",
            ));
        }
        Ok(ChildCreationEffectOutcome::Created(Box::new(
            child_creation_evidence(&request, result.session_id, result.workspace_lease)?,
        )))
    }

    fn reconcile_creation(
        &self,
        request: ChildCreationDispatchRequest,
    ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError> {
        let validated = validate_child_dispatch_request(&request, self.parent_session_id)?;
        let matches = self
            .data
            .list(ListSessionsDataRequest {
                sessions_root: self.sessions_root.clone(),
                limit: MAX_CHILDREN_PER_NODE as usize,
            })
            .map_err(|error| child_graph_effect_error(&format!("child_list:{error}")))?
            .into_iter()
            .filter(|record| {
                record.child_parent_session_id == Some(self.parent_session_id)
                    && record.child_parent_action_sequence
                        == Some(request.parent_action_sequence.get())
            })
            .collect::<Vec<_>>();
        let [child] = matches.as_slice() else {
            return Ok(if matches.is_empty() {
                ChildCreationEffectOutcome::Waiting {
                    code: String::from("child_creation_receipt_missing"),
                }
            } else {
                ChildCreationEffectOutcome::Ambiguous {
                    code: String::from("multiple_children_for_dispatch"),
                }
            });
        };
        if child.child_task_id.as_deref() != Some(request.identity.task_id.as_str()) {
            return Ok(ChildCreationEffectOutcome::Ambiguous {
                code: String::from("child_catalog_identity_substitution"),
            });
        }
        let loaded = SessionPersistenceLogic::new(self.data.clone())
            .load_session(LoadSessionCommand {
                session_directory: self.sessions_root.join(child.id.to_string()),
                expected_session_id: child.id,
            })
            .map_err(|error| child_graph_effect_error(&format!("child_replay:{error}")))?;
        if !replayed_child_matches(
            &loaded.state,
            &request,
            self.parent_session_id,
            &validated.task,
        ) {
            return Ok(ChildCreationEffectOutcome::Ambiguous {
                code: String::from("child_journal_identity_substitution"),
            });
        }
        let Some(workspace_lease) = loaded.state.workspace_lease else {
            return Ok(ChildCreationEffectOutcome::Ambiguous {
                code: String::from("child_workspace_lease_missing"),
            });
        };
        Ok(ChildCreationEffectOutcome::Created(Box::new(
            child_creation_evidence(&request, child.id, workspace_lease)?,
        )))
    }

    fn observe_terminal(
        &self,
        request: ChildTerminalObservationRequest,
    ) -> Result<ChildTerminalObservationOutcome, ChildGraphEffectError> {
        let parent = SessionPersistenceLogic::new(self.data.clone())
            .load_session(LoadSessionCommand {
                session_directory: self.parent_session_directory.clone(),
                expected_session_id: self.parent_session_id,
            })
            .map_err(|error| child_graph_effect_error(&format!("parent_replay:{error}")))?;
        let Some(record) = parent
            .state
            .child_agents
            .get(&request.identity.execution_id)
        else {
            return Ok(ChildTerminalObservationOutcome::Ambiguous {
                code: String::from("parent_child_receipt_missing"),
            });
        };
        let expected_link = generic_child_link_hash(
            &request.identity,
            request.child_session_id,
            record.proposed_at,
            &record.child_style,
        )
        .map_err(|error| child_graph_effect_error(&format!("child_link:{error}")))?;
        if record.generic_identity.as_deref() != Some(&request.identity)
            || record.child_session_id != Some(request.child_session_id)
            || expected_link != request.child_link_hash
        {
            return Ok(ChildTerminalObservationOutcome::Ambiguous {
                code: String::from("parent_child_receipt_substitution"),
            });
        }
        let child = SessionPersistenceLogic::new(self.data.clone())
            .load_session(LoadSessionCommand {
                session_directory: self
                    .sessions_root
                    .join(request.child_session_id.to_string()),
                expected_session_id: request.child_session_id,
            })
            .map_err(|error| child_graph_effect_error(&format!("child_replay:{error}")))?;
        let Some(origin) = child.state.child_origin.as_ref() else {
            return Ok(ChildTerminalObservationOutcome::Ambiguous {
                code: String::from("child_origin_missing"),
            });
        };
        let Some(contract) = record.generic.as_deref() else {
            return Ok(ChildTerminalObservationOutcome::Ambiguous {
                code: String::from("parent_child_contract_missing"),
            });
        };
        if origin.parent_session_id != self.parent_session_id
            || origin.parent_action_sequence != record.proposed_at
            || origin.parent_graph_node_id != request.identity.work.node_id
            || origin.task_id != request.identity.task_id
            || origin.inherited_provider != contract.inherited_provider
            || origin.inherited_model != contract.inherited_model
            || origin.inherited_mcp != contract.inherited_mcp
            || origin.artifact_references != contract.artifact_references
        {
            return Ok(ChildTerminalObservationOutcome::Ambiguous {
                code: String::from("child_origin_substitution"),
            });
        }
        let project_artifacts = parent
            .state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.execution_contract.as_deref())
            .and_then(|contract| {
                contract
                    .node_executors
                    .iter()
                    .find(|resolution| resolution.node_id == request.identity.work.node_id)
            })
            .is_some_and(|resolution| {
                resolution.executor_id == "runtime.child-spawn"
                    && resolution.executor_version == "1.1.0"
            });
        terminal_observation_from_replay(&request, &child.state, project_artifacts)
    }

    fn cancel_after_dispatch(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError> {
        self.validate_cancellation_dispatch(&request)?;
        for child_session_id in &request.identity.child_session_ids {
            let directory = self.sessions_root.join(child_session_id.to_string());
            let loaded = SessionPersistenceLogic::new(self.data.clone())
                .load_session(LoadSessionCommand {
                    session_directory: directory.clone(),
                    expected_session_id: *child_session_id,
                })
                .map_err(|error| {
                    child_graph_effect_error(&format!("child_cancel_replay:{error}"))
                })?;
            if !child_owned_by_parent(
                &loaded.state,
                self.parent_session_id,
                &request.identity,
                *child_session_id,
                &self.parent_session_directory,
                &self.data,
            )? || loaded.state.lifecycle != SessionLifecycle::Active
            {
                return Ok(ChildCancellationEffectOutcome::Ambiguous {
                    code: String::from("child_cancellation_preexisting_or_substituted"),
                });
            }
            if let Some(cancellation_id) = loaded
                .state
                .style_execution
                .as_ref()
                .and_then(|execution| execution.latest_model_execution.as_ref())
                .filter(|execution| execution.completed_at.is_none())
                .map(|execution| execution.cancellation_id.clone())
            {
                self.data
                    .request_runtime_cancellation(RequestRuntimeCancellationDataCommand {
                        cancellation_id,
                    })
                    .map_err(|error| {
                        child_graph_effect_error(&format!("child_provider_cancel:{error}"))
                    })?;
            }
            append_child_cancelled(
                &self.data,
                directory,
                loaded.state,
                loaded.last_event_id,
                &request.identity.cancellation_id,
            )?;
        }
        self.reconcile_cancellation(request)
    }

    fn reconcile_cancellation(
        &self,
        request: ChildCancellationDispatchRequest,
    ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError> {
        self.validate_cancellation_dispatch(&request)?;
        let mut children = Vec::with_capacity(request.identity.child_session_ids.len());
        for child_session_id in &request.identity.child_session_ids {
            let loaded = SessionPersistenceLogic::new(self.data.clone())
                .load_session(LoadSessionCommand {
                    session_directory: self.sessions_root.join(child_session_id.to_string()),
                    expected_session_id: *child_session_id,
                })
                .map_err(|error| {
                    child_graph_effect_error(&format!("child_cancel_replay:{error}"))
                })?;
            if !child_owned_by_parent(
                &loaded.state,
                self.parent_session_id,
                &request.identity,
                *child_session_id,
                &self.parent_session_directory,
                &self.data,
            )? || loaded.state.lifecycle != SessionLifecycle::Cancelled
            {
                return Ok(ChildCancellationEffectOutcome::Ambiguous {
                    code: String::from("child_cancellation_receipt_missing_or_partial"),
                });
            }
            children.push(GenericChildCancellationChildReceipt {
                child_session_id: *child_session_id,
                child_head_sequence: loaded.state.last_sequence,
            });
        }
        Ok(ChildCancellationEffectOutcome::Completed { children })
    }
}

impl<D> RuntimeChildGraphChildSessions<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + NodeExecutorDataPort
        + RuntimeCancellationControlDataPort
        + SessionRegistryDataPort
        + SessionStyleDataPort
        + 'static,
{
    fn validate_cancellation_dispatch(
        &self,
        request: &ChildCancellationDispatchRequest,
    ) -> Result<(), ChildGraphEffectError> {
        let parent = SessionPersistenceLogic::new(self.data.clone())
            .load_session(LoadSessionCommand {
                session_directory: self.parent_session_directory.clone(),
                expected_session_id: self.parent_session_id,
            })
            .map_err(|error| child_graph_effect_error(&format!("parent_replay:{error}")))?;
        let Some(record) = parent
            .state
            .planner_worker
            .child_cancellations
            .get(&request.identity.cancellation_id)
        else {
            return Err(child_graph_effect_error("cancellation_outbox_missing"));
        };
        if record.identity != request.identity
            || record.action_digest != Some(request.action_digest)
            || record.dispatch_hash != Some(request.dispatch_hash)
            || record.state != GenericChildCancellationState::Dispatched
        {
            return Err(child_graph_effect_error("cancellation_outbox_substitution"));
        }
        Ok(())
    }
}

fn child_owned_by_parent<D>(
    child: &SessionState,
    parent_session_id: SessionId,
    cancellation: &GenericChildCancellationIdentity,
    child_session_id: SessionId,
    parent_session_directory: &std::path::Path,
    data: &D,
) -> Result<bool, ChildGraphEffectError>
where
    D: Clone + JournalEventDataPort,
{
    let Some(origin) = child.child_origin.as_ref() else {
        return Ok(false);
    };
    if origin.parent_session_id != parent_session_id
        || origin.parent_graph_node_id.trim().is_empty()
    {
        return Ok(false);
    }
    let parent = SessionPersistenceLogic::new(data.clone())
        .load_session(LoadSessionCommand {
            session_directory: parent_session_directory.to_path_buf(),
            expected_session_id: parent_session_id,
        })
        .map_err(|error| child_graph_effect_error(&format!("parent_replay:{error}")))?;
    Ok(cancellation.child_session_ids.contains(&child_session_id)
        && parent.state.child_agents.values().any(|record| {
            record.child_session_id == Some(child_session_id)
                && record.proposed_at == origin.parent_action_sequence
                && record.identity.task_id == origin.task_id
                && record.state == ChildAgentState::Active
        }))
}

fn append_child_cancelled<D>(
    data: &D,
    session_directory: PathBuf,
    state: SessionState,
    last_event_id: EventId,
    cancellation_id: &str,
) -> Result<(), ChildGraphEffectError>
where
    D: Clone + EventIdentityDataPort + JournalEventDataPort,
{
    let identity = data
        .allocate_event_identity(AllocateEventIdentityDataRequest)
        .map_err(|error| child_graph_effect_error(&format!("child_cancel_identity:{error}")))?;
    let sequence = state
        .last_sequence
        .checked_next()
        .map_err(|_| child_graph_effect_error("child_cancel_sequence"))?;
    let payload = RuntimeCommittedEvent::SessionLifecycleChanged(SessionLifecycleChangedEvent {
        lifecycle: SessionLifecycle::Cancelled,
        reason: Some(format!("parent_child_cancellation:{cancellation_id}")),
    });
    let event = EventEnvelope::seal(
        EventMetadata {
            event_id: identity.event_id,
            scope: EventScope::Session(state.id),
            sequence,
            timestamp: identity.timestamp,
            event_type: payload.event_type().to_owned(),
            event_version: Version::new(1, 0),
            correlation_id: identity.correlation_id,
            causation_id: CausationId::from_uuid(last_event_id.into_uuid()),
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
    .map_err(|_| child_graph_effect_error("child_cancel_event"))?;
    reduce(Some(state), &event)
        .map_err(|error| child_graph_effect_error(&format!("child_cancel_reduce:{error}")))?;
    match SessionPersistenceLogic::new(data.clone())
        .compare_append_event(CompareAppendSessionEventCommand {
            session_directory,
            expected_head_event_id: last_event_id,
            event,
            durability: CommitDurability::Full,
        })
        .map_err(|error| child_graph_effect_error(&format!("child_cancel_append:{error}")))?
    {
        CompareAppendSessionEventResult::Appended(_) => Ok(()),
        CompareAppendSessionEventResult::Conflict => {
            Err(child_graph_effect_error("child_cancel_append_conflict"))
        }
    }
}

fn validate_child_dispatch_request(
    request: &ChildCreationDispatchRequest,
    parent_session_id: SessionId,
) -> Result<ValidatedChildDispatch, ChildGraphEffectError> {
    let task = child_task_text(&request.contract.task)?;
    let task_bytes = serde_json::to_vec(&request.contract.task)
        .map_err(|_| child_graph_effect_error("child_task_encoding"))?;
    let workspace_mode = workspace_lease_mode(&request.contract.workspace)?;
    let expected_action = generic_child_action_digest(
        &request.identity,
        &request.contract,
        &request.child_style,
        request.token_budget,
    )
    .map_err(|error| child_graph_effect_error(&format!("child_action:{error}")))?;
    let expected_dispatch = generic_child_dispatch_hash(&request.identity, expected_action)
        .map_err(|error| child_graph_effect_error(&format!("child_dispatch:{error}")))?;
    if request.contract.parent_session_id != parent_session_id
        || request.identity.work.branch_path.len() > 64
        || request.contract.task_hash != ContentHash::digest(&task_bytes)
        || request.action_digest != expected_action
        || request.dispatch_hash != expected_dispatch
        || request.parent_action_sequence == Sequence::FIRST
        || request.contract.context_budget_tokens == 0
        || request.contract.context_budget_tokens > request.token_budget
        || request.contract.cost_budget_micros == 0
        || request.contract.artifact_references.len() > 256
    {
        return Err(child_graph_effect_error("invalid_child_dispatch"));
    }
    Ok(ValidatedChildDispatch {
        task,
        workspace_mode,
    })
}

struct ValidatedChildDispatch {
    task: String,
    workspace_mode: WorkspaceLeaseMode,
}

fn workspace_lease_mode(
    workspace: &serde_json::Value,
) -> Result<WorkspaceLeaseMode, ChildGraphEffectError> {
    let workspace = workspace
        .as_object()
        .ok_or_else(|| child_graph_effect_error("invalid_child_workspace"))?;
    match workspace.get("mode").and_then(serde_json::Value::as_str) {
        Some("shared_read_only") => Ok(WorkspaceLeaseMode::SharedReadOnly),
        Some("temporary_copy" | "isolated_copy") => Ok(WorkspaceLeaseMode::IsolatedCopy),
        Some("branch_workspace") => {
            let merge_policy = match workspace
                .get("merge_policy")
                .and_then(serde_json::Value::as_str)
            {
                Some("manual_review") => WorkspaceMergePolicy::ManualReview,
                Some("reviewed_fast_forward") => WorkspaceMergePolicy::ReviewedFastForward,
                Some("reviewed_three_way") => WorkspaceMergePolicy::ReviewedThreeWay,
                _ => return Err(child_graph_effect_error("invalid_child_workspace")),
            };
            Ok(WorkspaceLeaseMode::BranchWorkspace { merge_policy })
        }
        _ => Err(child_graph_effect_error("unsupported_child_workspace")),
    }
}

fn child_task_text(task: &serde_json::Value) -> Result<String, ChildGraphEffectError> {
    match task {
        serde_json::Value::String(task) => Ok(task.clone()),
        task => {
            serde_json::to_string(task).map_err(|_| child_graph_effect_error("child_task_encoding"))
        }
    }
}

fn child_creation_evidence(
    request: &ChildCreationDispatchRequest,
    child_session_id: SessionId,
    workspace_lease: crate::workspace::WorkspaceLeaseContract,
) -> Result<ChildCreationEvidence, ChildGraphEffectError> {
    Ok(ChildCreationEvidence {
        child_session_id,
        parent_action_sequence: request.parent_action_sequence,
        child_link_hash: generic_child_link_hash(
            &request.identity,
            child_session_id,
            request.parent_action_sequence,
            &request.child_style,
        )
        .map_err(|error| child_graph_effect_error(&format!("child_link:{error}")))?,
        workspace_lease,
    })
}

fn replayed_child_matches(
    state: &SessionState,
    request: &ChildCreationDispatchRequest,
    parent_session_id: SessionId,
    task: &str,
) -> bool {
    let Some(origin) = state.child_origin.as_ref() else {
        return false;
    };
    let style_matches = state.style_binding.as_ref().is_some_and(|binding| {
        (request.child_style == binding.id
            || request.child_style == format!("{}@{}", binding.id, binding.version)
            || request
                .child_style
                .strip_prefix(&binding.id)
                .is_some_and(|suffix| suffix.starts_with('@')))
            && binding.budgets.max_tokens <= request.token_budget
            && binding.budgets.max_tokens <= request.contract.context_budget_tokens
            && binding.budgets.max_cost_micros <= request.contract.cost_budget_micros
            && binding
                .tool_groups
                .iter()
                .all(|group| request.contract.tool_groups.contains(group))
            && binding.mcp == request.contract.inherited_mcp.clone().unwrap_or_default()
    });
    origin.parent_session_id == parent_session_id
        && origin.parent_action_sequence == request.parent_action_sequence
        && origin.parent_graph_node_id == request.identity.work.node_id
        && origin.task_id == request.identity.task_id
        && origin.revision == request.identity.work.loop_iteration
        && origin.depth == request.contract.depth
        && origin.task == task
        && origin.input_hash == ContentHash::digest(task.as_bytes())
        && origin.token_budget == request.token_budget
        && origin.inherited_provider == request.contract.inherited_provider
        && origin.inherited_model == request.contract.inherited_model
        && origin.inherited_mcp == request.contract.inherited_mcp
        && origin.artifact_references == request.contract.artifact_references
        && style_matches
}

fn terminal_observation_from_replay(
    request: &ChildTerminalObservationRequest,
    state: &SessionState,
    project_artifacts: bool,
) -> Result<ChildTerminalObservationOutcome, ChildGraphEffectError> {
    let disposition = match state.lifecycle {
        SessionLifecycle::Active | SessionLifecycle::Suspended => {
            return Ok(ChildTerminalObservationOutcome::Pending);
        }
        SessionLifecycle::Completed => GenericChildTerminalDisposition::Completed,
        SessionLifecycle::Failed => GenericChildTerminalDisposition::Failed,
        SessionLifecycle::Cancelled => GenericChildTerminalDisposition::Cancelled,
        SessionLifecycle::Archived => {
            return Ok(ChildTerminalObservationOutcome::Ambiguous {
                code: String::from("archived_child_terminal_unknown"),
            });
        }
    };
    let result_reference = (disposition == GenericChildTerminalDisposition::Completed).then(|| {
        format!(
            "child-session:{}@{}#{}",
            request.child_session_id,
            state.last_sequence.get(),
            state.last_event_checksum
        )
    });
    let failure_code = (disposition == GenericChildTerminalDisposition::Failed).then(|| {
        state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.termination_reason.clone())
            .unwrap_or_else(|| String::from("child_session_failed"))
    });
    let artifact_references =
        if project_artifacts && disposition == GenericChildTerminalDisposition::Completed {
            let completion = state
                .successful_session_completion
                .as_ref()
                .ok_or_else(|| child_graph_effect_error("child_completion_boundary_missing"))?;
            let references = state
                .artifact_persistences
                .values()
                .filter(|record| {
                    record
                        .completed_at
                        .is_some_and(|sequence| sequence <= completion.sequence)
                })
                .filter_map(|record| record.artifact_reference.clone())
                .collect::<BTreeSet<_>>();
            if references.len() > 256
                || references
                    .iter()
                    .any(|reference| reference.len() > MAX_REFERENCE_BYTES)
            {
                return Err(child_graph_effect_error(
                    "child_artifact_projection_invalid",
                ));
            }
            references
        } else {
            BTreeSet::new()
        };
    let mut receipt = GenericChildTerminalReceipt {
        disposition,
        result_reference: result_reference.clone(),
        artifact_references: artifact_references.clone(),
        failure_code: failure_code.clone(),
        receipt_hash: ContentHash::from_bytes([0; 32]),
    };
    receipt.receipt_hash = generic_child_terminal_receipt_hash(
        &request.identity,
        request.child_session_id,
        state.last_sequence,
        &receipt,
    )
    .map_err(|error| child_graph_effect_error(&format!("child_terminal:{error}")))?;
    Ok(ChildTerminalObservationOutcome::Terminal(
        ChildTerminalEvidence {
            child_session_id: request.child_session_id,
            child_head_sequence: state.last_sequence,
            disposition,
            result_reference,
            artifact_references,
            failure_code,
            receipt_hash: receipt.receipt_hash,
        },
    ))
}

fn child_graph_effect_error(code: &str) -> ChildGraphEffectError {
    let bounded = if code.len() > MAX_REFERENCE_BYTES {
        String::from("child_graph_boundary_failure")
    } else {
        code.to_owned()
    };
    ChildGraphEffectError { code: bounded }
}

/// Stable effect-boundary failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("child graph effect failed: {code}")]
pub struct ChildGraphEffectError {
    /// Bounded diagnostic code.
    pub code: String,
}

/// Complete bounded command for one exact persisted child graph executor.
#[derive(Clone, Debug, PartialEq)]
pub struct CoordinateChildGraphTurnCommand {
    /// Canonical parent session.
    pub session_id: SessionId,
    /// Exact graph-node work.
    pub work: NodeWorkIdentity,
    /// Immutable execution-plan hash.
    pub execution_plan_hash: ContentHash,
    /// Exact persisted executor resolution.
    pub executor: SessionNodeExecutorResolution,
    /// Complete compiled-node/configuration hash.
    pub configuration_hash: ContentHash,
    /// Pure style-independent node outcome.
    pub outcome: ChildGraphNodeOutcome,
    /// Maximum dispatched or active children at once.
    pub maximum_in_flight: u32,
    /// Maximum additional canonically proposed children.
    pub maximum_queued: u32,
}

/// Successful coordinator disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildGraphTurnResult {
    /// Canonical application reached a stable completed projection.
    Applied {
        /// Number of events appended by this invocation.
        appended_events: usize,
    },
    /// Execution is waiting for a typed external condition.
    Waiting {
        /// Stable bounded reason.
        reason: String,
        /// Opaque continuation/wait reference when one exists.
        continuation_reference: Option<String>,
        /// Number of events appended before waiting.
        appended_events: usize,
    },
    /// Policy terminally denied a consequential proposal.
    Denied {
        /// Stable bounded denial code.
        code: String,
        /// Number of events appended before denial.
        appended_events: usize,
    },
    /// An external effect may have happened and must not be redispatched.
    Ambiguous {
        /// Stable bounded ambiguity code.
        code: String,
        /// Number of events appended before ambiguity.
        appended_events: usize,
    },
    /// A terminal boundary failure was proven.
    Failed {
        /// Stable bounded failure code.
        code: String,
        /// Number of events appended before failure.
        appended_events: usize,
    },
}

/// Durable child graph coordinator over injected journal and effect ports.
pub struct ChildGraphTurnCoordinator<J, E> {
    journal: J,
    effects: E,
}

impl<J, E> ChildGraphTurnCoordinator<J, E> {
    /// Creates a coordinator without assembling concrete dependencies.
    #[must_use]
    pub const fn new(journal: J, effects: E) -> Self {
        Self { journal, effects }
    }
}

impl<J, E> ChildGraphTurnCoordinator<J, E>
where
    J: ChildGraphTurnJournal,
    E: ChildGraphEffectPort,
{
    /// Executes or recovers one child graph node from canonical replay.
    ///
    /// # Errors
    ///
    /// Fails closed on executor/work/plan/configuration substitution, invalid
    /// application plans, reducer rejection, unbounded values, journal
    /// failures, or exhausted compare-and-swap retries.
    pub async fn coordinate(
        &self,
        command: &CoordinateChildGraphTurnCommand,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        validate_command_bounds(command)?;
        match &command.outcome {
            ChildGraphNodeOutcome::Spawn { proposals } => {
                self.coordinate_spawn(command, proposals).await
            }
            ChildGraphNodeOutcome::Wait(projection) => {
                self.coordinate_wait(command, projection).await
            }
            ChildGraphNodeOutcome::Review(routing) => {
                self.coordinate_review(command, routing).await
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the method keeps every durable child creation phase visibly ordered"
    )]
    async fn coordinate_spawn(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        proposals: &[ChildSpawnProposal],
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        validate_spawn_bounds(command, proposals)?;
        let mut appended = 0;
        let mut freshly_dispatched = BTreeSet::new();

        appended += self.append_application_phase(command, None, PhaseSelection::Proposed)?;

        let proposed = self.bound_records(command)?;
        let mut approvals = BTreeMap::new();
        let approval_slots = available_approval_slots(&proposed, command.maximum_in_flight)?;
        for record in proposed
            .iter()
            .filter(|record| record.state == ChildAgentState::Proposed)
            .take(approval_slots)
        {
            let request = authorization_request(record)?;
            match self.effects.authorize_creation(request.clone()).await? {
                ChildCreationAuthorizationOutcome::Approved { action_digest } => {
                    if action_digest != request.action_digest {
                        return Err(ChildGraphTurnError::EffectSubstitution);
                    }
                    approvals.insert(record.identity.task_id.clone(), action_digest);
                }
                ChildCreationAuthorizationOutcome::Waiting {
                    continuation_reference,
                } => {
                    validate_reference(&continuation_reference)?;
                    return Ok(ChildGraphTurnResult::Waiting {
                        reason: String::from("child_creation_approval"),
                        continuation_reference: Some(continuation_reference),
                        appended_events: appended,
                    });
                }
                ChildCreationAuthorizationOutcome::Denied { code } => {
                    validate_reference(&code)?;
                    return Ok(ChildGraphTurnResult::Denied {
                        code,
                        appended_events: appended,
                    });
                }
            }
        }
        if !approvals.is_empty() {
            appended += self.append_application_phase(
                command,
                Some(&ChildGraphApplicationEvidence {
                    approvals,
                    ..ChildGraphApplicationEvidence::default()
                }),
                PhaseSelection::Approved,
            )?;
        }

        let before_dispatch = self.journal.load()?;
        validate_loaded_identity(&before_dispatch.state, command)?;
        let available =
            available_dispatch_slots(&before_dispatch.state, command, command.maximum_in_flight)?;
        if available > 0 {
            let plan = application_plan(&before_dispatch.state, command, None)?;
            let selected = plan
                .into_iter()
                .filter_map(|event| match &event {
                    RuntimeCommittedEvent::GenericChildCreationDispatched(dispatched) => {
                        Some((dispatched.identity.task_id.clone(), event))
                    }
                    _ => None,
                })
                .take(available)
                .collect::<Vec<_>>();
            for (task_id, event) in selected {
                if self.append_exact(command, &event)? {
                    appended += 1;
                    freshly_dispatched.insert(task_id);
                }
            }
        }

        let dispatched = self.bound_records(command)?;
        let mut creations = BTreeMap::new();
        for record in dispatched
            .iter()
            .filter(|record| record.state == ChildAgentState::Dispatched)
        {
            let request = dispatch_request(record)?;
            let outcome = if freshly_dispatched.contains(&record.identity.task_id) {
                self.effects.create_after_dispatch(request).await?
            } else {
                self.effects.reconcile_creation(request).await?
            };
            match outcome {
                ChildCreationEffectOutcome::Created(receipt) => {
                    creations.insert(record.identity.task_id.clone(), *receipt);
                }
                ChildCreationEffectOutcome::Waiting { code } => {
                    validate_reference(&code)?;
                    return Ok(ChildGraphTurnResult::Waiting {
                        reason: code,
                        continuation_reference: None,
                        appended_events: appended,
                    });
                }
                ChildCreationEffectOutcome::Ambiguous { code } => {
                    validate_reference(&code)?;
                    return Ok(ChildGraphTurnResult::Ambiguous {
                        code,
                        appended_events: appended,
                    });
                }
                ChildCreationEffectOutcome::Failed { code } => {
                    validate_reference(&code)?;
                    return Ok(ChildGraphTurnResult::Failed {
                        code,
                        appended_events: appended,
                    });
                }
            }
        }
        if !creations.is_empty() {
            appended += self.append_application_phase(
                command,
                Some(&ChildGraphApplicationEvidence {
                    creations,
                    ..ChildGraphApplicationEvidence::default()
                }),
                PhaseSelection::Created,
            )?;
        }

        let final_records = self.bound_records(command)?;
        if final_records.iter().all(|record| {
            matches!(
                record.state,
                ChildAgentState::Active
                    | ChildAgentState::Completed
                    | ChildAgentState::Failed
                    | ChildAgentState::Cancelled
            )
        }) {
            Ok(ChildGraphTurnResult::Applied {
                appended_events: appended,
            })
        } else {
            Ok(ChildGraphTurnResult::Waiting {
                reason: String::from("child_completion"),
                continuation_reference: None,
                appended_events: appended,
            })
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the wait state machine keeps terminal observation, canonical projection, and cancellation proposal ordering explicit"
    )]
    async fn coordinate_wait(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        projection: &ChildWaitProjection,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        let mut appended = 0;
        let active = self.bound_wait_records(command, projection)?;
        let mut terminals = BTreeMap::new();
        for record in active
            .iter()
            .filter(|record| record.state == ChildAgentState::Active)
        {
            let request = terminal_request(record)?;
            match self.effects.observe_terminal(request).await? {
                ChildTerminalObservationOutcome::Pending => {}
                ChildTerminalObservationOutcome::Terminal(receipt) => {
                    terminals.insert(record.identity.task_id.clone(), receipt);
                }
                ChildTerminalObservationOutcome::Ambiguous { code } => {
                    validate_reference(&code)?;
                    return Ok(ChildGraphTurnResult::Ambiguous {
                        code,
                        appended_events: appended,
                    });
                }
            }
        }
        if !terminals.is_empty() {
            appended += self.append_application_phase(
                command,
                Some(&ChildGraphApplicationEvidence {
                    terminals,
                    ..ChildGraphApplicationEvidence::default()
                }),
                PhaseSelection::Terminal,
            )?;
            return Ok(ChildGraphTurnResult::Waiting {
                reason: String::from("child_terminal_observed"),
                continuation_reference: None,
                appended_events: appended,
            });
        }
        appended += self.append_application_phase(command, None, PhaseSelection::Wait)?;
        match projection {
            ChildWaitProjection::Waiting { .. } => Ok(ChildGraphTurnResult::Waiting {
                reason: String::from("child_wait"),
                continuation_reference: None,
                appended_events: appended,
            }),
            ChildWaitProjection::Completed { .. } => Ok(ChildGraphTurnResult::Applied {
                appended_events: appended,
            }),
            ChildWaitProjection::Failed {
                code,
                cancel_children,
                ..
            } if cancel_children.is_empty() => Ok(ChildGraphTurnResult::Failed {
                code: code.clone(),
                appended_events: appended,
            }),
            ChildWaitProjection::Failed {
                code,
                cancel_children,
                ..
            } => {
                let projection_hash = self.committed_wait_projection_hash(command)?;
                let request = ChildCancellationProposalRequest {
                    session_id: command.session_id,
                    work: command.work.clone(),
                    execution_plan_hash: command.execution_plan_hash,
                    configuration_hash: command.configuration_hash,
                    projection_hash,
                    reason: code.clone(),
                    child_ids: cancel_children.clone(),
                };
                let record = if let Some(record) = self.cancellation_record(command, &request)? {
                    record
                } else {
                    let identity = child_cancellation_identity(&request)?;
                    let requested = RuntimeCommittedEvent::GenericChildCancellationRequested(
                        Box::new(GenericChildCancellationRequestedEvent { identity }),
                    );
                    appended += usize::from(self.append_exact(command, &requested)?);
                    self.cancellation_record(command, &request)?
                        .ok_or(ChildGraphTurnError::EffectSubstitution)?
                };
                self.recover_cancellation(command, code, &request, record, appended)
                    .await
            }
        }
    }

    async fn authorize_cancellation(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        failure_code: &str,
        request: &ChildCancellationProposalRequest,
        mut appended: usize,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        match self.effects.propose_cancellation(request.clone()).await? {
            ChildCancellationProposalOutcome::Proposed { proposal_reference } => {
                validate_reference(&proposal_reference)?;
                let action_digest = cancellation_action_digest(&proposal_reference)?;
                let identity = child_cancellation_identity(request)?;
                let authorized = RuntimeCommittedEvent::GenericChildCancellationAuthorized(
                    Box::new(GenericChildCancellationAuthorizedEvent {
                        identity: identity.clone(),
                        action_digest,
                    }),
                );
                appended += usize::from(self.append_exact(command, &authorized)?);
                self.dispatch_authorized_cancellation(
                    command,
                    failure_code,
                    identity,
                    action_digest,
                    appended,
                )
                .await
            }
            ChildCancellationProposalOutcome::Waiting {
                continuation_reference,
            } => {
                validate_reference(&continuation_reference)?;
                Ok(ChildGraphTurnResult::Waiting {
                    reason: String::from("child_cancellation_approval"),
                    continuation_reference: Some(continuation_reference),
                    appended_events: appended,
                })
            }
            ChildCancellationProposalOutcome::Denied { code } => {
                validate_reference(&code)?;
                Ok(ChildGraphTurnResult::Denied {
                    code,
                    appended_events: appended,
                })
            }
            ChildCancellationProposalOutcome::Ambiguous { code } => {
                validate_reference(&code)?;
                Ok(ChildGraphTurnResult::Ambiguous {
                    code,
                    appended_events: appended,
                })
            }
        }
    }

    fn cancellation_record(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        request: &ChildCancellationProposalRequest,
    ) -> Result<Option<GenericChildCancellationRecord>, ChildGraphTurnError> {
        let head = self.journal.load()?;
        validate_loaded_identity(&head.state, command)?;
        let expected = child_cancellation_identity(request)?;
        let record = head
            .state
            .planner_worker
            .child_cancellations
            .get(&expected.cancellation_id)
            .cloned();
        if record
            .as_ref()
            .is_some_and(|record| record.identity != expected)
        {
            return Err(ChildGraphTurnError::EffectSubstitution);
        }
        Ok(record)
    }

    async fn recover_cancellation(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        failure_code: &str,
        request: &ChildCancellationProposalRequest,
        record: GenericChildCancellationRecord,
        appended: usize,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        match record.state {
            GenericChildCancellationState::Requested => {
                self.authorize_cancellation(command, failure_code, request, appended)
                    .await
            }
            GenericChildCancellationState::Authorized => {
                self.dispatch_authorized_cancellation(
                    command,
                    failure_code,
                    record.identity,
                    record
                        .action_digest
                        .ok_or(ChildGraphTurnError::EffectSubstitution)?,
                    appended,
                )
                .await
            }
            GenericChildCancellationState::Completed => Ok(ChildGraphTurnResult::Failed {
                code: failure_code.to_owned(),
                appended_events: appended,
            }),
            GenericChildCancellationState::Ambiguous => Ok(ChildGraphTurnResult::Ambiguous {
                code: record
                    .ambiguity_code
                    .unwrap_or_else(|| String::from("child_cancellation_ambiguous")),
                appended_events: appended,
            }),
            GenericChildCancellationState::Dispatched => {
                self.finish_cancellation_effect(
                    command,
                    failure_code,
                    ChildCancellationDispatchRequest {
                        identity: record.identity,
                        action_digest: record
                            .action_digest
                            .ok_or(ChildGraphTurnError::EffectSubstitution)?,
                        dispatch_hash: record
                            .dispatch_hash
                            .ok_or(ChildGraphTurnError::EffectSubstitution)?,
                    },
                    false,
                    appended,
                )
                .await
            }
        }
    }

    async fn dispatch_authorized_cancellation(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        failure_code: &str,
        identity: GenericChildCancellationIdentity,
        action_digest: ContentHash,
        mut appended: usize,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        let dispatch_hash = generic_child_cancellation_dispatch_hash(&identity, action_digest)?;
        let dispatch = ChildCancellationDispatchRequest {
            identity: identity.clone(),
            action_digest,
            dispatch_hash,
        };
        let outbox = RuntimeCommittedEvent::GenericChildCancellationDispatched(Box::new(
            GenericChildCancellationDispatchedEvent {
                identity,
                action_digest,
                dispatch_hash,
            },
        ));
        let fresh = self.append_exact(command, &outbox)?;
        appended += usize::from(fresh);
        self.finish_cancellation_effect(command, failure_code, dispatch, fresh, appended)
            .await
    }

    async fn finish_cancellation_effect(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        failure_code: &str,
        request: ChildCancellationDispatchRequest,
        fresh_dispatch: bool,
        mut appended: usize,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        let effect = if fresh_dispatch {
            self.effects.cancel_after_dispatch(request.clone()).await
        } else {
            self.effects.reconcile_cancellation(request.clone()).await
        };
        let outcome = match effect {
            Ok(outcome) => outcome,
            Err(_) => ChildCancellationEffectOutcome::Ambiguous {
                code: String::from("child_cancellation_boundary_ambiguous"),
            },
        };
        match outcome {
            ChildCancellationEffectOutcome::Completed { children } => {
                if children
                    .iter()
                    .map(|child| child.child_session_id)
                    .ne(request.identity.child_session_ids.iter().copied())
                    || children
                        .windows(2)
                        .any(|pair| pair[0].child_session_id >= pair[1].child_session_id)
                {
                    return self.append_cancellation_ambiguity(
                        command,
                        request,
                        "child_cancellation_receipt_substitution",
                        appended,
                    );
                }
                let mut receipt = GenericChildCancellationReceipt {
                    children,
                    receipt_hash: ContentHash::from_bytes([0; 32]),
                };
                receipt.receipt_hash = generic_child_cancellation_receipt_hash(
                    &request.identity,
                    request.action_digest,
                    request.dispatch_hash,
                    &receipt,
                )?;
                let event = RuntimeCommittedEvent::GenericChildCancellationCompleted(Box::new(
                    GenericChildCancellationCompletedEvent {
                        identity: request.identity,
                        action_digest: request.action_digest,
                        dispatch_hash: request.dispatch_hash,
                        receipt,
                    },
                ));
                appended += usize::from(self.append_exact(command, &event)?);
                Ok(ChildGraphTurnResult::Failed {
                    code: failure_code.to_owned(),
                    appended_events: appended,
                })
            }
            ChildCancellationEffectOutcome::Ambiguous { code } => {
                validate_reference(&code)?;
                self.append_cancellation_ambiguity(command, request, &code, appended)
            }
        }
    }

    fn append_cancellation_ambiguity(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        request: ChildCancellationDispatchRequest,
        code: &str,
        mut appended: usize,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        let event = RuntimeCommittedEvent::GenericChildCancellationAmbiguous(Box::new(
            GenericChildCancellationAmbiguousEvent {
                identity: request.identity,
                action_digest: request.action_digest,
                dispatch_hash: request.dispatch_hash,
                code: code.to_owned(),
            },
        ));
        appended += usize::from(self.append_exact(command, &event)?);
        Ok(ChildGraphTurnResult::Ambiguous {
            code: code.to_owned(),
            appended_events: appended,
        })
    }

    async fn coordinate_review(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        routing: &ReviewRoutingProposal,
    ) -> Result<ChildGraphTurnResult, ChildGraphTurnError> {
        let request = ChildReviewEvidenceRequest {
            session_id: command.session_id,
            work: command.work.clone(),
            execution_plan_hash: command.execution_plan_hash,
            configuration_hash: command.configuration_hash,
            routing: routing.clone(),
        };
        match self.effects.validate_review_evidence(request).await? {
            ChildReviewEvidenceOutcome::Validated { evidence_hash }
                if evidence_hash == routing.evidence_hash =>
            {
                let appended =
                    self.append_application_phase(command, None, PhaseSelection::Review)?;
                Ok(ChildGraphTurnResult::Applied {
                    appended_events: appended,
                })
            }
            ChildReviewEvidenceOutcome::Validated { .. } => {
                Err(ChildGraphTurnError::EffectSubstitution)
            }
            ChildReviewEvidenceOutcome::Waiting {
                continuation_reference,
            } => {
                validate_reference(&continuation_reference)?;
                Ok(ChildGraphTurnResult::Waiting {
                    reason: String::from("review_evidence"),
                    continuation_reference: Some(continuation_reference),
                    appended_events: 0,
                })
            }
            ChildReviewEvidenceOutcome::Rejected { code } => {
                validate_reference(&code)?;
                Ok(ChildGraphTurnResult::Failed {
                    code,
                    appended_events: 0,
                })
            }
            ChildReviewEvidenceOutcome::Ambiguous { code } => {
                validate_reference(&code)?;
                Ok(ChildGraphTurnResult::Ambiguous {
                    code,
                    appended_events: 0,
                })
            }
        }
    }

    fn append_application_phase(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        evidence: Option<&ChildGraphApplicationEvidence>,
        selection: PhaseSelection,
    ) -> Result<usize, ChildGraphTurnError> {
        let mut appended = 0;
        for _ in 0..MAX_APPEND_RETRIES {
            let head = self.journal.load()?;
            validate_loaded_identity(&head.state, command)?;
            let events = application_plan(&head.state, command, evidence)?;
            let selected = events
                .into_iter()
                .filter(|event| selection.matches(event))
                .collect::<Vec<_>>();
            if selected.is_empty() {
                return Ok(appended);
            }
            let mut conflict = false;
            for event in selected {
                match self.append_at_head(&self.journal.load()?, command, event)? {
                    ChildGraphTurnAppendOutcome::Appended => appended += 1,
                    ChildGraphTurnAppendOutcome::Conflict => {
                        conflict = true;
                        break;
                    }
                }
            }
            if !conflict {
                return Ok(appended);
            }
        }
        Err(ChildGraphTurnError::AppendConflictLimit)
    }

    fn append_exact(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        event: &RuntimeCommittedEvent,
    ) -> Result<bool, ChildGraphTurnError> {
        for _ in 0..MAX_APPEND_RETRIES {
            let head = self.journal.load()?;
            validate_loaded_identity(&head.state, command)?;
            match self.append_at_head(&head, command, event.clone())? {
                ChildGraphTurnAppendOutcome::Appended => return Ok(true),
                ChildGraphTurnAppendOutcome::Conflict => {
                    let refreshed = self.journal.load()?;
                    if event_already_applied(&refreshed.state, event) {
                        return Ok(false);
                    }
                }
            }
        }
        Err(ChildGraphTurnError::AppendConflictLimit)
    }

    fn append_at_head(
        &self,
        head: &ChildGraphTurnHead,
        command: &CoordinateChildGraphTurnCommand,
        payload: RuntimeCommittedEvent,
    ) -> Result<ChildGraphTurnAppendOutcome, ChildGraphTurnError> {
        validate_loaded_identity(&head.state, command)?;
        let identity = self.journal.allocate_identity()?;
        let sequence = head
            .state
            .last_sequence
            .checked_next()
            .map_err(|_| ChildGraphTurnError::Sequence)?;
        let event = EventEnvelope::seal(
            EventMetadata {
                event_id: identity.event_id,
                scope: EventScope::Session(command.session_id),
                sequence,
                timestamp: identity.timestamp,
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: identity.correlation_id,
                causation_id: CausationId::from_uuid(head.last_event_id.into_uuid()),
                parent_graph_node_id: Some(command.work.node_id.clone()),
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
        .map_err(|_| ChildGraphTurnError::Event)?;
        reduce(Some(head.state.clone()), &event)?;
        self.journal
            .append(
                ChildGraphTurnAppendPosition {
                    sequence: head.state.last_sequence,
                    event_id: head.last_event_id,
                },
                event,
            )
            .map_err(ChildGraphTurnError::Journal)
    }

    fn bound_records(
        &self,
        command: &CoordinateChildGraphTurnCommand,
    ) -> Result<Vec<ChildAgentRecord>, ChildGraphTurnError> {
        let head = self.journal.load()?;
        validate_loaded_identity(&head.state, command)?;
        let mut records = head
            .state
            .child_agents
            .values()
            .filter(|record| {
                record
                    .generic_identity
                    .as_deref()
                    .is_some_and(|identity| identity.work == command.work)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.identity.task_id.cmp(&right.identity.task_id));
        Ok(records)
    }

    fn bound_wait_records(
        &self,
        command: &CoordinateChildGraphTurnCommand,
        projection: &ChildWaitProjection,
    ) -> Result<Vec<ChildAgentRecord>, ChildGraphTurnError> {
        let expected = match projection {
            ChildWaitProjection::Waiting { pending, .. } => pending,
            ChildWaitProjection::Failed {
                cancel_children, ..
            } => cancel_children,
            ChildWaitProjection::Completed { .. } => return Ok(Vec::new()),
        }
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
        let head = self.journal.load()?;
        validate_loaded_identity(&head.state, command)?;
        let mut records = head
            .state
            .child_agents
            .values()
            .filter(|record| {
                record.generic_identity.as_deref().is_some_and(|identity| {
                    identity.execution_plan_hash == command.execution_plan_hash
                        && record
                            .child_session_id
                            .is_some_and(|child_id| expected.contains(&child_id))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if records.len() != expected.len() {
            return Err(ChildGraphTurnError::Identity);
        }
        records.sort_by(|left, right| left.identity.task_id.cmp(&right.identity.task_id));
        Ok(records)
    }

    fn committed_wait_projection_hash(
        &self,
        command: &CoordinateChildGraphTurnCommand,
    ) -> Result<ContentHash, ChildGraphTurnError> {
        let head = self.journal.load()?;
        validate_loaded_identity(&head.state, command)?;
        head.state
            .planner_worker
            .child_waits
            .values()
            .find(|record| record.projection.work == command.work)
            .map(|record| record.projection.projection_hash)
            .ok_or(ChildGraphTurnError::Identity)
    }
}

#[derive(Clone, Copy)]
enum PhaseSelection {
    Proposed,
    Approved,
    Created,
    Terminal,
    Wait,
    Review,
}

impl PhaseSelection {
    fn matches(self, event: &RuntimeCommittedEvent) -> bool {
        matches!(
            (self, event),
            (
                Self::Proposed,
                RuntimeCommittedEvent::GenericChildCreationProposed(_)
            ) | (
                Self::Approved,
                RuntimeCommittedEvent::GenericChildCreationApproved(_)
            ) | (Self::Created, RuntimeCommittedEvent::GenericChildCreated(_))
                | (
                    Self::Terminal,
                    RuntimeCommittedEvent::GenericChildTerminal(_)
                )
                | (Self::Wait, RuntimeCommittedEvent::ChildWaitProjected(_))
                | (Self::Review, RuntimeCommittedEvent::GenericReviewRouted(_))
        )
    }
}

fn validate_command_bounds(
    command: &CoordinateChildGraphTurnCommand,
) -> Result<(), ChildGraphTurnError> {
    if command.session_id.into_uuid().is_nil()
        || command.work.run_id.trim().is_empty()
        || command.work.node_id.trim().is_empty()
        || command.work.attempt == 0
        || command.work.step == 0
        || command.execution_plan_hash == ContentHash::from_bytes([0; 32])
        || command.configuration_hash == ContentHash::from_bytes([0; 32])
        || command.executor.node_id != command.work.node_id
        || command.executor.adapter_configuration_reference != command.configuration_hash
        || command.maximum_in_flight == 0
        || command.maximum_queued > MAX_CHILDREN_PER_NODE
    {
        return Err(ChildGraphTurnError::InvalidCommand);
    }
    Ok(())
}

fn validate_spawn_bounds(
    command: &CoordinateChildGraphTurnCommand,
    proposals: &[ChildSpawnProposal],
) -> Result<(), ChildGraphTurnError> {
    let bound = command
        .maximum_in_flight
        .checked_add(command.maximum_queued)
        .ok_or(ChildGraphTurnError::InvalidCommand)?;
    if proposals.is_empty() || proposals.len() > 1_024 || proposals.len() > bound as usize {
        return Err(ChildGraphTurnError::InvalidCommand);
    }
    Ok(())
}

fn validate_loaded_identity(
    state: &SessionState,
    command: &CoordinateChildGraphTurnCommand,
) -> Result<(), ChildGraphTurnError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(ChildGraphTurnError::Identity)?;
    let contract = execution
        .execution_contract
        .as_deref()
        .ok_or(ChildGraphTurnError::Identity)?;
    let resolution = contract
        .node_executors
        .iter()
        .find(|resolution| resolution.node_id == command.work.node_id)
        .ok_or(ChildGraphTurnError::Identity)?;
    if state.id != command.session_id
        || contract.run_id != command.work.run_id
        || contract.execution_plan_hash != command.execution_plan_hash
        || resolution != &command.executor
        || resolution.adapter_configuration_reference != command.configuration_hash
    {
        return Err(ChildGraphTurnError::Identity);
    }
    Ok(())
}

fn application_plan(
    state: &SessionState,
    command: &CoordinateChildGraphTurnCommand,
    evidence: Option<&ChildGraphApplicationEvidence>,
) -> Result<Vec<RuntimeCommittedEvent>, ChildGraphTurnError> {
    Ok(plan_child_graph_application(
        state,
        &PlanChildGraphApplicationCommand {
            session_id: command.session_id,
            work: command.work.clone(),
            execution_plan_hash: command.execution_plan_hash,
            configuration_hash: command.configuration_hash,
            outcome: command.outcome.clone(),
            evidence: evidence.cloned().unwrap_or_default(),
        },
    )?
    .events)
}

fn authorization_request(
    record: &ChildAgentRecord,
) -> Result<ChildCreationAuthorizationRequest, ChildGraphTurnError> {
    let identity = record
        .generic_identity
        .as_deref()
        .ok_or(ChildGraphTurnError::Identity)?
        .clone();
    let contract = record
        .generic
        .as_deref()
        .ok_or(ChildGraphTurnError::Identity)?
        .clone();
    let action_digest = generic_child_action_digest(
        &identity,
        &contract,
        &record.child_style,
        record.token_budget,
    )?;
    Ok(ChildCreationAuthorizationRequest {
        identity,
        contract,
        child_style: record.child_style.clone(),
        token_budget: record.token_budget,
        action_digest,
        proposed_at: record.proposed_at,
    })
}

fn dispatch_request(
    record: &ChildAgentRecord,
) -> Result<ChildCreationDispatchRequest, ChildGraphTurnError> {
    let authorization = authorization_request(record)?;
    Ok(ChildCreationDispatchRequest {
        identity: authorization.identity,
        contract: authorization.contract,
        child_style: authorization.child_style,
        token_budget: authorization.token_budget,
        action_digest: record.action_digest.ok_or(ChildGraphTurnError::Identity)?,
        dispatch_hash: record.dispatch_hash.ok_or(ChildGraphTurnError::Identity)?,
        parent_action_sequence: record.proposed_at,
        dispatched_at: record.dispatched_at.ok_or(ChildGraphTurnError::Identity)?,
    })
}

fn terminal_request(
    record: &ChildAgentRecord,
) -> Result<ChildTerminalObservationRequest, ChildGraphTurnError> {
    let identity = record
        .generic_identity
        .as_deref()
        .ok_or(ChildGraphTurnError::Identity)?
        .clone();
    let child_session_id = record
        .child_session_id
        .ok_or(ChildGraphTurnError::Identity)?;
    Ok(ChildTerminalObservationRequest {
        child_link_hash: generic_child_link_hash(
            &identity,
            child_session_id,
            record.proposed_at,
            &record.child_style,
        )?,
        identity,
        child_session_id,
    })
}

fn available_dispatch_slots(
    state: &SessionState,
    command: &CoordinateChildGraphTurnCommand,
    maximum: u32,
) -> Result<usize, ChildGraphTurnError> {
    let in_flight = state
        .child_agents
        .values()
        .filter(|record| {
            record
                .generic_identity
                .as_deref()
                .is_some_and(|identity| identity.work == command.work)
                && matches!(
                    record.state,
                    ChildAgentState::Dispatched | ChildAgentState::Active
                )
        })
        .count();
    (maximum as usize)
        .checked_sub(in_flight)
        .ok_or(ChildGraphTurnError::InvalidCommand)
}

fn available_approval_slots(
    records: &[ChildAgentRecord],
    maximum: u32,
) -> Result<usize, ChildGraphTurnError> {
    let occupying = records
        .iter()
        .filter(|record| {
            matches!(
                record.state,
                ChildAgentState::Approved | ChildAgentState::Dispatched | ChildAgentState::Active
            )
        })
        .count();
    (maximum as usize)
        .checked_sub(occupying)
        .ok_or(ChildGraphTurnError::InvalidCommand)
}

fn event_already_applied(state: &SessionState, event: &RuntimeCommittedEvent) -> bool {
    match event {
        RuntimeCommittedEvent::GenericChildCreationDispatched(dispatched) => state
            .child_agents
            .get(&dispatched.identity.execution_id)
            .is_some_and(|record| {
                record.dispatch_hash == Some(dispatched.dispatch_hash)
                    && matches!(
                        record.state,
                        ChildAgentState::Dispatched
                            | ChildAgentState::Active
                            | ChildAgentState::Completed
                            | ChildAgentState::Failed
                            | ChildAgentState::Cancelled
                    )
            }),
        _ => false,
    }
}

fn validate_reference(value: &str) -> Result<(), ChildGraphTurnError> {
    if value.trim().is_empty()
        || value.len() > MAX_REFERENCE_BYTES
        || value.chars().any(char::is_control)
    {
        Err(ChildGraphTurnError::InvalidEffectOutcome)
    } else {
        Ok(())
    }
}

fn child_cancellation_identity(
    request: &ChildCancellationProposalRequest,
) -> Result<GenericChildCancellationIdentity, ChildGraphTurnError> {
    let mut child_session_ids = request.child_ids.clone();
    child_session_ids.sort();
    if child_session_ids.is_empty() || child_session_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ChildGraphTurnError::InvalidEffectOutcome);
    }
    let mut identity = GenericChildCancellationIdentity {
        cancellation_id: String::new(),
        work: request.work.clone(),
        execution_plan_hash: request.execution_plan_hash,
        configuration_hash: request.configuration_hash,
        projection_hash: request.projection_hash,
        reason_hash: ContentHash::digest(request.reason.as_bytes()),
        child_session_ids,
    };
    let identity_hash = generic_child_cancellation_identity_hash(&identity)?;
    identity.cancellation_id = format!("generic-child-cancellation:{identity_hash}");
    Ok(identity)
}

fn cancellation_action_digest(
    proposal_reference: &str,
) -> Result<ContentHash, ChildGraphTurnError> {
    let Some(digest) = proposal_reference.strip_prefix("child-agent-cancellation:") else {
        return Err(ChildGraphTurnError::EffectSubstitution);
    };
    ContentHash::from_str(digest).map_err(|_| ChildGraphTurnError::EffectSubstitution)
}

/// Durable child graph coordination failure.
#[derive(Debug, Error)]
pub enum ChildGraphTurnError {
    /// Command bounds or required identities are invalid.
    #[error("child graph turn command is invalid")]
    InvalidCommand,
    /// Replay differs from the exact persisted executor/work/plan/configuration.
    #[error("child graph turn identity does not match canonical replay")]
    Identity,
    /// An effect returned a different identity or digest.
    #[error("child graph effect substituted canonical identity")]
    EffectSubstitution,
    /// An effect returned an empty, oversized, or unsafe diagnostic reference.
    #[error("child graph effect outcome is invalid")]
    InvalidEffectOutcome,
    /// Event sequence allocation overflowed.
    #[error("child graph event sequence overflow")]
    Sequence,
    /// Canonical event sealing failed.
    #[error("child graph event sealing failed")]
    Event,
    /// CAS conflicts exceeded the bounded retry limit.
    #[error("child graph append conflict retry limit exceeded")]
    AppendConflictLimit,
    /// Pure child graph application planning failed.
    #[error(transparent)]
    Application(#[from] ChildGraphApplicationError),
    /// Shared canonical reducer rejected a planned event.
    #[error(transparent)]
    Reducer(#[from] SessionReducerError),
    /// Canonical journal boundary failed.
    #[error(transparent)]
    Journal(#[from] ChildGraphTurnJournalError),
    /// Child effect boundary failed.
    #[error(transparent)]
    Effect(#[from] ChildGraphEffectError),
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use agentmod_graph_engine::{
        CompilerLimits, ExecutableGraph, GraphCacheInputs, SecurityClassification,
        compile as compile_graph,
    };
    use agentmod_runtime_data::{
        fixture_file::{
            CreateFixtureDirectoryDataRequest, FixtureFileDataPort, ListFixtureDirectoryDataRequest,
        },
        local::{local_runtime_data, local_runtime_data_with_node_executors},
        node_executor::RuntimeNodeExecutorData,
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        child_graph_continuation::ChildGraphAncillaryApplicationPhase,
        child_graph_execution::{ResolvedChildWorkspace, ReviewDisposition},
        session::{
            GenericChildTerminalDisposition, GenericChildTerminalReceipt, SessionCreatedEvent,
            SessionLifecycleChangedEvent, SessionNodeExecutorBoundary, SessionNodeExecutorSource,
            SessionStyleBudgets, StyleExecutionContract, StyleExecutionInitializedEvent,
            StyleNodeEnteredEvent, generic_child_link_hash, generic_child_terminal_receipt_hash,
        },
        style::{StyleDecisionCapability, StyleHarnessDescriptor},
    };

    const SPAWN_GRAPH: &str = r#"
format_version = 1
entry = "dispatch-work"
[budget]
max_steps = 32
max_tokens = 10000
max_cost_micros = 100000
max_duration_ms = 60000
[declarations]
capabilities = ["agents"]
providers = []

[[nodes]]
id = "dispatch-work"
kind = "spawn_child_agent"
configuration = { type = "spawn_child_agent", task_input = { kind = "static", value = "work" }, task_id_prefix = "task", child_style = "worker@1.0.0", tool_groups = [], maximum_children = 8, maximum_depth = 2, token_budget = 1000, context_budget_tokens = 500, cost_budget_micros = 10000, workspace = { mode = "shared_read_only" }, artifact_references = [], security_classification = "internal", approval_required = true }

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "dispatch-work"
to = "done"
"#;

    fn session_id() -> SessionId {
        SessionId::from_uuid(
            Uuid::from_str("018f6f83-7b80-7000-8000-000000000071").expect("session UUID"),
        )
    }

    fn compile_spawn_graph() -> ExecutableGraph {
        compile_graph(
            SPAWN_GRAPH,
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"child-turn-plugins"),
                runtime_api_version: String::from("1.0.0"),
                capability_set: BTreeSet::from([String::from("agents")]),
            },
            CompilerLimits::default(),
        )
        .expect("spawn graph")
    }

    fn resolution(graph: &ExecutableGraph) -> SessionNodeExecutorResolution {
        let node = &graph.nodes[graph.entry_index];
        SessionNodeExecutorResolution {
            node_id: node.id.clone(),
            node_kind: String::from("spawn_child_agent"),
            executor_id: String::from("runtime.child-spawn"),
            executor_version: String::from("1.0.0"),
            source: SessionNodeExecutorSource::Runtime,
            boundary: SessionNodeExecutorBoundary::RuntimeLogic,
            required_capabilities: vec![String::from("agents")],
            resolved_capabilities: vec![String::from("agents")],
            runtime_api_requirement: String::from("^1.0.0"),
            executor_declaration_hash: ContentHash::digest(b"runtime.child-spawn@1.0.0"),
            adapter_configuration_reference: ContentHash::digest(
                &serde_json::to_vec(node).expect("node"),
            ),
        }
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(10_000 + u128::from(sequence))),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(20_000)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(30_000)),
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

    fn initial_head() -> (ChildGraphTurnHead, CoordinateChildGraphTurnCommand) {
        let graph = compile_spawn_graph();
        let executor = resolution(&graph);
        let work = NodeWorkIdentity {
            run_id: String::from("child-turn-run"),
            node_id: String::from("dispatch-work"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        };
        let plan_hash = ContentHash::digest(b"child-turn-plan");
        let mut state = reduce(
            None,
            &envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: String::from("arbitrary-user-graph"),
                    style_binding: None,
                }),
            ),
        )
        .expect("created");
        state = reduce(
            Some(state),
            &envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(graph.clone()),
                        input_reference: None,
                        execution_contract: None,
                    },
                )),
            ),
        )
        .expect("initialized");
        state = reduce(
            Some(state),
            &envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: work.node_id.clone(),
                    attempt: work.attempt,
                    loop_iteration: work.loop_iteration,
                    step: work.step,
                }),
            ),
        )
        .expect("entered");
        state
            .style_execution
            .as_mut()
            .expect("execution")
            .execution_contract = Some(Box::new(StyleExecutionContract {
            style_binding_hash: ContentHash::digest(b"binding"),
            execution_plan_hash: plan_hash,
            registry_hash: ContentHash::digest(b"registry"),
            node_executors: vec![executor.clone()],
            initial_node_id: work.node_id.clone(),
            initial_variables_json: String::from("{}"),
            invocation_provider: Some(String::from("mock")),
            invocation_model: Some(String::from("mock-model")),
            invocation_options_json: None,
            initial_budgets: SessionStyleBudgets {
                max_iterations: 8,
                max_steps: 32,
                max_tokens: 10_000,
                max_cost_micros: 100_000,
                max_duration_ms: 60_000,
            },
            run_id: work.run_id.clone(),
        }));
        let proposals = vec![
            proposal(&work, "task-b"),
            proposal(&work, "task-a"),
            proposal(&work, "task-c"),
        ];
        (
            ChildGraphTurnHead {
                state,
                last_event_id: EventId::from_uuid(Uuid::from_u128(10_003)),
            },
            CoordinateChildGraphTurnCommand {
                session_id: session_id(),
                work,
                execution_plan_hash: plan_hash,
                configuration_hash: executor.adapter_configuration_reference,
                executor,
                outcome: ChildGraphNodeOutcome::Spawn { proposals },
                maximum_in_flight: 2,
                maximum_queued: 2,
            },
        )
    }

    fn proposal(work: &NodeWorkIdentity, task_id: &str) -> ChildSpawnProposal {
        proposal_with_task(
            work,
            task_id,
            serde_json::json!({"instruction": format!("complete {task_id}")}),
        )
    }

    fn proposal_with_task(
        work: &NodeWorkIdentity,
        task_id: &str,
        task: serde_json::Value,
    ) -> ChildSpawnProposal {
        let mut proposal = ChildSpawnProposal {
            parent_session_id: session_id(),
            work: work.clone(),
            task_id: task_id.to_owned(),
            task_hash: ContentHash::digest(&serde_json::to_vec(&task).expect("task")),
            task,
            child_style: String::from("worker@1.0.0"),
            inherited_provider: None,
            inherited_model: None,
            inherited_mcp: None,
            tool_groups: BTreeSet::new(),
            depth: 1,
            token_budget: 1000,
            context_budget_tokens: 500,
            cost_budget_micros: 10_000,
            workspace: ResolvedChildWorkspace::SharedReadOnly,
            artifact_references: BTreeSet::new(),
            security_classification: SecurityClassification::Internal,
            approval_required: true,
            proposal_hash: ContentHash::from_bytes([0; 32]),
        };
        proposal.proposal_hash =
            ContentHash::digest(&serde_json::to_vec(&proposal).expect("zero proposal"));
        proposal
    }

    #[derive(Default)]
    struct JournalState {
        head: Option<ChildGraphTurnHead>,
        appended_types: Vec<String>,
        conflicts_remaining: usize,
        crash_after_commit: Option<usize>,
        successful_appends: usize,
        identities: u128,
    }

    #[derive(Clone)]
    struct FakeJournal {
        state: Arc<Mutex<JournalState>>,
    }

    impl FakeJournal {
        fn new(head: ChildGraphTurnHead) -> Self {
            Self {
                state: Arc::new(Mutex::new(JournalState {
                    head: Some(head),
                    identities: 40_000,
                    ..JournalState::default()
                })),
            }
        }

        fn canonical_state(&self) -> SessionState {
            self.state
                .lock()
                .expect("journal")
                .head
                .as_ref()
                .expect("head")
                .state
                .clone()
        }
    }

    impl ChildGraphTurnJournal for FakeJournal {
        fn load(&self) -> Result<ChildGraphTurnHead, ChildGraphTurnJournalError> {
            Ok(self
                .state
                .lock()
                .expect("journal")
                .head
                .as_ref()
                .expect("head")
                .clone())
        }

        fn allocate_identity(
            &self,
        ) -> Result<ChildGraphTurnEventIdentity, ChildGraphTurnJournalError> {
            let mut state = self.state.lock().expect("journal");
            state.identities += 1;
            Ok(ChildGraphTurnEventIdentity {
                event_id: EventId::from_uuid(Uuid::from_u128(state.identities)),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(20_000)),
            })
        }

        fn append(
            &self,
            expected: ChildGraphTurnAppendPosition,
            event: EventEnvelope<RuntimeCommittedEvent>,
        ) -> Result<ChildGraphTurnAppendOutcome, ChildGraphTurnJournalError> {
            let mut state = self.state.lock().expect("journal");
            if state.conflicts_remaining > 0 {
                state.conflicts_remaining -= 1;
                return Ok(ChildGraphTurnAppendOutcome::Conflict);
            }
            let head = state.head.as_ref().expect("head");
            if head.state.last_sequence != expected.sequence
                || head.last_event_id != expected.event_id
            {
                return Ok(ChildGraphTurnAppendOutcome::Conflict);
            }
            let next = reduce(Some(head.state.clone()), &event)
                .map_err(|_| journal_error("fake_reducer"))?;
            let event_type = event.payload.event_type().to_owned();
            let event_id = event.metadata.event_id;
            state.head = Some(ChildGraphTurnHead {
                state: next,
                last_event_id: event_id,
            });
            state.appended_types.push(event_type);
            state.successful_appends += 1;
            if state.crash_after_commit == Some(state.successful_appends) {
                state.crash_after_commit = None;
                return Err(journal_error("crash_after_commit"));
            }
            Ok(ChildGraphTurnAppendOutcome::Appended)
        }
    }

    fn journal_error(code: &str) -> ChildGraphTurnJournalError {
        ChildGraphTurnJournalError {
            code: code.to_owned(),
        }
    }

    #[derive(Clone)]
    struct FakeEffects {
        state: Arc<Mutex<EffectState>>,
    }

    struct EffectState {
        authorization: ChildCreationAuthorizationOutcome,
        create_outcome: Option<ChildCreationEffectOutcome>,
        create_after_effect_error_once: bool,
        terminals: BTreeSet<String>,
        create_calls: BTreeMap<String, usize>,
        reconcile_calls: BTreeMap<String, usize>,
        authorization_order: Vec<String>,
        cancellation: ChildCancellationProposalOutcome,
        cancellation_effect: Option<ChildCancellationEffectOutcome>,
        cancellation_receipt_available: bool,
        cancellation_dispatch_calls: usize,
        cancellation_reconcile_calls: usize,
        review: Option<ChildReviewEvidenceOutcome>,
    }

    impl Default for EffectState {
        fn default() -> Self {
            Self {
                authorization: ChildCreationAuthorizationOutcome::Approved {
                    action_digest: ContentHash::from_bytes([0; 32]),
                },
                create_outcome: None,
                create_after_effect_error_once: false,
                terminals: BTreeSet::new(),
                create_calls: BTreeMap::new(),
                reconcile_calls: BTreeMap::new(),
                authorization_order: Vec::new(),
                cancellation: ChildCancellationProposalOutcome::Proposed {
                    proposal_reference: format!(
                        "child-agent-cancellation:{}",
                        ContentHash::digest(b"accepted-cancellation")
                    ),
                },
                cancellation_effect: None,
                cancellation_receipt_available: false,
                cancellation_dispatch_calls: 0,
                cancellation_reconcile_calls: 0,
                review: None,
            }
        }
    }

    impl FakeEffects {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(EffectState::default())),
            }
        }

        fn creation_receipt(request: &ChildCreationDispatchRequest) -> ChildCreationEvidence {
            let suffix = request
                .identity
                .task_id
                .bytes()
                .fold(0_u128, |sum, byte| sum + u128::from(byte));
            let child_session_id = SessionId::from_uuid(Uuid::from_u128(50_000 + suffix));
            ChildCreationEvidence {
                child_session_id,
                parent_action_sequence: request.parent_action_sequence,
                child_link_hash: generic_child_link_hash(
                    &request.identity,
                    child_session_id,
                    request.parent_action_sequence,
                    &request.child_style,
                )
                .expect("link"),
                workspace_lease: crate::workspace::test_workspace_lease(
                    crate::workspace::WorkspaceLeaseOwner {
                        parent_session_id: request.contract.parent_session_id,
                        parent_action_sequence: request.parent_action_sequence,
                        parent_graph_node_id: request.identity.work.node_id.clone(),
                        task_id: request.identity.task_id.clone(),
                    },
                    PathBuf::from("fixture"),
                ),
            }
        }
    }

    #[async_trait]
    impl ChildGraphEffectPort for FakeEffects {
        async fn authorize_creation(
            &self,
            request: ChildCreationAuthorizationRequest,
        ) -> Result<ChildCreationAuthorizationOutcome, ChildGraphEffectError> {
            let mut state = self.state.lock().expect("effects");
            state
                .authorization_order
                .push(request.identity.task_id.clone());
            Ok(match &state.authorization {
                ChildCreationAuthorizationOutcome::Approved { .. } => {
                    ChildCreationAuthorizationOutcome::Approved {
                        action_digest: request.action_digest,
                    }
                }
                other => other.clone(),
            })
        }

        async fn create_after_dispatch(
            &self,
            request: ChildCreationDispatchRequest,
        ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError> {
            let mut state = self.state.lock().expect("effects");
            *state
                .create_calls
                .entry(request.identity.task_id.clone())
                .or_default() += 1;
            if state.create_after_effect_error_once {
                state.create_after_effect_error_once = false;
                return Err(effect_error("crash_after_create"));
            }
            Ok(state.create_outcome.clone().unwrap_or_else(|| {
                ChildCreationEffectOutcome::Created(Box::new(Self::creation_receipt(&request)))
            }))
        }

        async fn reconcile_creation(
            &self,
            request: ChildCreationDispatchRequest,
        ) -> Result<ChildCreationEffectOutcome, ChildGraphEffectError> {
            let mut state = self.state.lock().expect("effects");
            *state
                .reconcile_calls
                .entry(request.identity.task_id.clone())
                .or_default() += 1;
            Ok(ChildCreationEffectOutcome::Created(Box::new(
                Self::creation_receipt(&request),
            )))
        }

        async fn observe_terminal(
            &self,
            request: ChildTerminalObservationRequest,
        ) -> Result<ChildTerminalObservationOutcome, ChildGraphEffectError> {
            let state = self.state.lock().expect("effects");
            if !state.terminals.contains(&request.identity.task_id) {
                return Ok(ChildTerminalObservationOutcome::Pending);
            }
            let child_head_sequence = Sequence::new(77).expect("sequence");
            let mut receipt = GenericChildTerminalReceipt {
                disposition: GenericChildTerminalDisposition::Completed,
                result_reference: Some(format!("node-result:{}", request.identity.task_id)),
                artifact_references: BTreeSet::new(),
                failure_code: None,
                receipt_hash: ContentHash::from_bytes([0; 32]),
            };
            receipt.receipt_hash = generic_child_terminal_receipt_hash(
                &request.identity,
                request.child_session_id,
                child_head_sequence,
                &receipt,
            )
            .expect("terminal hash");
            Ok(ChildTerminalObservationOutcome::Terminal(
                ChildTerminalEvidence {
                    child_session_id: request.child_session_id,
                    child_head_sequence,
                    disposition: receipt.disposition,
                    result_reference: receipt.result_reference,
                    artifact_references: receipt.artifact_references,
                    failure_code: receipt.failure_code,
                    receipt_hash: receipt.receipt_hash,
                },
            ))
        }

        async fn propose_cancellation(
            &self,
            _request: ChildCancellationProposalRequest,
        ) -> Result<ChildCancellationProposalOutcome, ChildGraphEffectError> {
            Ok(self.state.lock().expect("effects").cancellation.clone())
        }

        async fn cancel_after_dispatch(
            &self,
            request: ChildCancellationDispatchRequest,
        ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError> {
            let mut state = self.state.lock().expect("effects");
            state.cancellation_dispatch_calls += 1;
            let outcome = state.cancellation_effect.clone().unwrap_or_else(|| {
                ChildCancellationEffectOutcome::Completed {
                    children: request
                        .identity
                        .child_session_ids
                        .into_iter()
                        .map(|child_session_id| GenericChildCancellationChildReceipt {
                            child_session_id,
                            child_head_sequence: Sequence::new(88).expect("sequence"),
                        })
                        .collect(),
                }
            });
            state.cancellation_receipt_available =
                matches!(outcome, ChildCancellationEffectOutcome::Completed { .. });
            Ok(outcome)
        }

        async fn reconcile_cancellation(
            &self,
            request: ChildCancellationDispatchRequest,
        ) -> Result<ChildCancellationEffectOutcome, ChildGraphEffectError> {
            let mut state = self.state.lock().expect("effects");
            state.cancellation_reconcile_calls += 1;
            if !state.cancellation_receipt_available {
                return Ok(ChildCancellationEffectOutcome::Ambiguous {
                    code: String::from("child_cancellation_receipt_missing_or_partial"),
                });
            }
            Ok(state.cancellation_effect.clone().unwrap_or_else(|| {
                ChildCancellationEffectOutcome::Completed {
                    children: request
                        .identity
                        .child_session_ids
                        .into_iter()
                        .map(|child_session_id| GenericChildCancellationChildReceipt {
                            child_session_id,
                            child_head_sequence: Sequence::new(88).expect("sequence"),
                        })
                        .collect(),
                }
            }))
        }

        async fn validate_review_evidence(
            &self,
            request: ChildReviewEvidenceRequest,
        ) -> Result<ChildReviewEvidenceOutcome, ChildGraphEffectError> {
            Ok(self
                .state
                .lock()
                .expect("effects")
                .review
                .clone()
                .unwrap_or(ChildReviewEvidenceOutcome::Validated {
                    evidence_hash: request.routing.evidence_hash,
                }))
        }
    }

    fn effect_error(code: &str) -> ChildGraphEffectError {
        ChildGraphEffectError {
            code: code.to_owned(),
        }
    }

    fn switch_to_cancel_wait(
        journal: &FakeJournal,
        mut command: CoordinateChildGraphTurnCommand,
    ) -> CoordinateChildGraphTurnCommand {
        let mut child_ids = journal
            .canonical_state()
            .child_agents
            .values()
            .filter_map(|record| record.child_session_id)
            .collect::<Vec<_>>();
        child_ids.sort();
        let child_id_toml = child_ids
            .iter()
            .map(|id| format!("\"{id}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let graph = compile_graph(
            &format!(
                r#"
format_version = 1
entry = "cancel-wait"
[budget]
max_steps = 32
max_tokens = 10000
max_cost_micros = 100000
max_duration_ms = 60000
[declarations]
capabilities = ["agents"]
providers = []

[[nodes]]
id = "cancel-wait"
kind = "wait_for_agents"
configuration = {{ type = "wait_for_agents", children = {{ kind = "exact", child_ids = [{child_id_toml}] }}, maximum_children = 8, minimum_successes = 1, timeout_ms = 60000, cancellation = "cascade" }}

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "cancel-wait"
to = "done"
"#
            ),
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"child-turn-plugins"),
                runtime_api_version: String::from("1.0.0"),
                capability_set: BTreeSet::from([String::from("agents")]),
            },
            CompilerLimits::default(),
        )
        .expect("wait graph");
        let node = &graph.nodes[graph.entry_index];
        let executor = SessionNodeExecutorResolution {
            node_id: node.id.clone(),
            node_kind: String::from("wait_for_agents"),
            executor_id: String::from("runtime.child-wait"),
            executor_version: String::from("1.0.0"),
            source: SessionNodeExecutorSource::Runtime,
            boundary: SessionNodeExecutorBoundary::RuntimeLogic,
            required_capabilities: vec![String::from("agents")],
            resolved_capabilities: vec![String::from("agents")],
            runtime_api_requirement: String::from("^1.0.0"),
            executor_declaration_hash: ContentHash::digest(b"runtime.child-wait@1.0.0"),
            adapter_configuration_reference: ContentHash::digest(
                &serde_json::to_vec(node).expect("node"),
            ),
        };
        command.work.node_id = String::from("cancel-wait");
        command.work.step = 2;
        command.configuration_hash = executor.adapter_configuration_reference;
        command.executor = executor.clone();
        let result_hash = ContentHash::digest(
            &serde_json::to_vec(&("parent_cancelled", &child_ids, false)).expect("wait result"),
        );
        command.outcome = ChildGraphNodeOutcome::Wait(ChildWaitProjection::Failed {
            code: String::from("parent_cancelled"),
            cancel_children: child_ids,
            detached: false,
            result_hash,
        });
        let mut journal_state = journal.state.lock().expect("journal");
        let head = journal_state.head.as_mut().expect("head");
        let execution = head.state.style_execution.as_mut().expect("execution");
        execution.graph = Box::new(graph);
        execution.active_node = Some(StyleNodeEnteredEvent {
            node_id: command.work.node_id.clone(),
            attempt: command.work.attempt,
            loop_iteration: command.work.loop_iteration,
            step: command.work.step,
        });
        let contract = execution.execution_contract.as_mut().expect("contract");
        contract.node_executors = vec![executor];
        contract.initial_node_id = command.work.node_id.clone();
        command
    }

    fn production_style_environment() -> StyleEnvironment {
        StyleEnvironment {
            runtime_api_version: String::from("1.0.0"),
            plugin_set_hash: ContentHash::digest(b"no-plugins").to_hex(),
            user_style_root: None,
            project_style_root: None,
            plugin_style_roots: Vec::new(),
            cache_root: None,
            capabilities: [
                "agents",
                "approval",
                "artifacts",
                "context",
                "events",
                "model",
                "scheduling",
                "tools",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            tool_groups: BTreeMap::from([(
                String::from("filesystem"),
                BTreeSet::from([
                    String::from("filesystem.read"),
                    String::from("filesystem.write"),
                ]),
            )]),
            providers: BTreeSet::from([String::from("deterministic-mock"), String::from("mock")]),
            plugins: BTreeSet::from([String::from("runtime.security")]),
            context_transforms: Vec::new(),
            plugin_memory_providers: Vec::new(),
            plugin_compactors: Vec::new(),
            memory_providers: BTreeSet::from([String::from("file"), String::from("none")]),
            compaction_strategies: [
                "artifact_handoff",
                "none",
                "sliding_window",
                "summary",
                "tool_output_eviction",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            supported_decisions: BTreeSet::from([
                StyleDecisionCapability::Continue,
                StyleDecisionCapability::Replace,
                StyleDecisionCapability::Reject,
                StyleDecisionCapability::RequireApproval,
                StyleDecisionCapability::Defer,
                StyleDecisionCapability::Cancel,
                StyleDecisionCapability::Fork,
            ]),
            graph_references: BTreeMap::new(),
            harnesses: BTreeMap::from([(
                String::from("native"),
                StyleHarnessDescriptor {
                    version: String::from("1.0.0"),
                    capabilities: [
                        "cancellation",
                        "streaming",
                        "structured_context_replacement",
                        "token_usage",
                        "tool_calls",
                    ]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                    available: true,
                },
            )]),
        }
    }

    async fn production_dispatch_request_for_task(
        task: serde_json::Value,
    ) -> ChildCreationDispatchRequest {
        let (head, mut command) = initial_head();
        command.outcome = ChildGraphNodeOutcome::Spawn {
            proposals: vec![proposal_with_task(&command.work, "task-a", task)],
        };
        command.maximum_in_flight = 1;
        let journal = FakeJournal::new(head);
        let effects = FakeEffects::new();
        effects.state.lock().expect("effects").create_outcome =
            Some(ChildCreationEffectOutcome::Waiting {
                code: String::from("hold_dispatch"),
            });
        let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects);
        assert!(matches!(
            coordinator.coordinate(&command).await.expect("dispatch"),
            ChildGraphTurnResult::Waiting { .. }
        ));
        let state = journal.canonical_state();
        let record = state
            .child_agents
            .values()
            .next()
            .expect("dispatched child");
        assert_eq!(record.state, ChildAgentState::Dispatched);
        let mut request = dispatch_request(record).expect("dispatch request");
        request.child_style = String::from("ephemeral-turn");
        request
            .contract
            .tool_groups
            .insert(String::from("filesystem"));
        request.contract.workspace = serde_json::json!({"mode":"isolated_copy"});
        request.contract.inherited_provider = Some(String::from("mock"));
        request.contract.inherited_model = Some(String::from("mock-model"));
        request.action_digest = generic_child_action_digest(
            &request.identity,
            &request.contract,
            &request.child_style,
            request.token_budget,
        )
        .expect("action digest");
        request.dispatch_hash =
            generic_child_dispatch_hash(&request.identity, request.action_digest)
                .expect("dispatch hash");
        request
    }

    async fn production_dispatch_request() -> ChildCreationDispatchRequest {
        production_dispatch_request_for_task(serde_json::json!({
            "instruction": "complete task-a"
        }))
        .await
    }

    fn directory_count(data: &impl FixtureFileDataPort, path: &std::path::Path) -> usize {
        data.list_fixture_directory(ListFixtureDirectoryDataRequest {
            directory: path.to_owned(),
        })
        .expect("session directory")
        .len()
    }

    #[test]
    fn child_task_text_preserves_strings_and_canonicalizes_structured_tasks() {
        assert_eq!(
            child_task_text(&serde_json::json!("inspect child recovery")).expect("string task"),
            "inspect child recovery"
        );
        assert_eq!(
            child_task_text(&serde_json::json!({
                "instruction": "inspect child recovery",
                "priority": 1
            }))
            .expect("structured task"),
            r#"{"instruction":"inspect child recovery","priority":1}"#
        );
    }

    #[test]
    fn production_journal_reloads_filesystem_and_rejects_stale_head() {
        let temporary = tempdir().expect("temporary directory");
        let session_directory = temporary.path().join("session");
        let data = local_runtime_data();
        let created = envelope(
            1,
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: String::from("fixture"),
                style: String::from("arbitrary-user-graph"),
                style_binding: None,
            }),
        );
        SessionPersistenceLogic::new(data.clone())
            .commit_event(crate::persistence::CommitSessionEventCommand {
                session_directory: session_directory.clone(),
                event: created.clone(),
                durability: CommitDurability::Full,
            })
            .expect("seed filesystem journal");
        let journal = SessionChildGraphTurnJournal::new(
            data.clone(),
            session_id(),
            session_directory.clone(),
        );
        let first = journal.load().expect("first replay");
        assert_eq!(first.state.last_sequence, Sequence::FIRST);
        let identity = journal.allocate_identity().expect("identity");
        let suspended = EventEnvelope::seal(
            EventMetadata {
                event_id: identity.event_id,
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(2).expect("sequence"),
                timestamp: identity.timestamp,
                event_type: String::from("session.lifecycle_changed"),
                event_version: Version::new(1, 0),
                correlation_id: identity.correlation_id,
                causation_id: CausationId::from_uuid(first.last_event_id.into_uuid()),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
                classification: EventClassification::Committed,
            },
            RuntimeCommittedEvent::SessionLifecycleChanged(SessionLifecycleChangedEvent {
                lifecycle: SessionLifecycle::Suspended,
                reason: Some(String::from("test_restart")),
            }),
        )
        .expect("suspended event");
        assert_eq!(
            journal
                .append(
                    ChildGraphTurnAppendPosition {
                        sequence: first.state.last_sequence,
                        event_id: first.last_event_id,
                    },
                    suspended.clone(),
                )
                .expect("append"),
            ChildGraphTurnAppendOutcome::Appended
        );
        let restarted =
            SessionChildGraphTurnJournal::new(data, session_id(), session_directory.clone());
        let replayed = restarted.load().expect("restart replay");
        assert_eq!(replayed.state.lifecycle, SessionLifecycle::Suspended);
        assert_eq!(replayed.state.last_sequence.get(), 2);
        assert_eq!(
            restarted
                .append(
                    ChildGraphTurnAppendPosition {
                        sequence: first.state.last_sequence,
                        event_id: first.last_event_id,
                    },
                    suspended,
                )
                .expect("stale append classification"),
            ChildGraphTurnAppendOutcome::Conflict
        );
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the production reconciliation fixture verifies creation, recovery, immutable invocation identity, and missing-child ambiguity together"
    )]
    async fn production_child_creation_reconciles_read_only_and_missing_stays_missing() {
        let request = production_dispatch_request().await;
        let temporary = tempdir().expect("temporary directory");
        let sessions_root = temporary.path().join("sessions");
        let workspace = temporary.path().join("workspace");
        let data = local_runtime_data_with_node_executors(
            RuntimeNodeExecutorData::native().expect("native executor registry"),
        );
        data.create_fixture_directory(CreateFixtureDirectoryDataRequest {
            directory: workspace.clone(),
            recursive: true,
        })
        .expect("workspace");
        let children = RuntimeChildGraphChildSessions::new(
            data,
            production_style_environment(),
            sessions_root.clone(),
            session_id(),
            sessions_root.join(session_id().to_string()),
            workspace.to_string_lossy().into_owned(),
            ChildMemoryAccess::None,
        );
        let created = children
            .create_after_dispatch(request.clone())
            .expect("create child");
        let ChildCreationEffectOutcome::Created(created) = created else {
            panic!("created child receipt");
        };
        let child_replay = SessionPersistenceLogic::new(local_runtime_data())
            .load_session(LoadSessionCommand {
                session_directory: sessions_root.join(created.child_session_id.to_string()),
                expected_session_id: created.child_session_id,
            })
            .expect("created child replay");
        let child_binding = child_replay
            .state
            .style_binding
            .as_ref()
            .expect("child binding");
        let child_origin = child_replay
            .state
            .child_origin
            .as_ref()
            .expect("child origin");
        assert_eq!(child_origin.inherited_provider.as_deref(), Some("mock"));
        assert_eq!(child_origin.inherited_model.as_deref(), Some("mock-model"));
        let compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&child_binding.compiled_style_json).expect("compiled child");
        assert!(
            compiled
                .graph
                .nodes
                .iter()
                .filter_map(|node| node.provider.as_deref())
                .all(|provider| provider == "mock")
        );
        assert!(
            child_binding.budgets.max_cost_micros <= request.contract.cost_budget_micros,
            "generic graph cost budget must lower the immutable child binding"
        );
        let initial_directory_count = directory_count(&children.data, &sessions_root);
        let recovered = children
            .reconcile_creation(request.clone())
            .expect("reconcile child");
        assert_eq!(
            recovered,
            ChildCreationEffectOutcome::Created(created.clone())
        );
        assert_eq!(
            directory_count(&children.data, &sessions_root),
            initial_directory_count
        );

        let mut substituted = request.clone();
        substituted.contract.inherited_model = Some(String::from("other-model"));
        substituted.action_digest = generic_child_action_digest(
            &substituted.identity,
            &substituted.contract,
            &substituted.child_style,
            substituted.token_budget,
        )
        .expect("substituted action");
        substituted.dispatch_hash =
            generic_child_dispatch_hash(&substituted.identity, substituted.action_digest)
                .expect("substituted dispatch");
        assert!(matches!(
            children
                .reconcile_creation(substituted)
                .expect("substituted reconciliation"),
            ChildCreationEffectOutcome::Ambiguous { .. }
        ));

        let mut missing = request;
        missing.parent_action_sequence =
            Sequence::new(missing.parent_action_sequence.get() + 100).expect("sequence");
        missing.action_digest = generic_child_action_digest(
            &missing.identity,
            &missing.contract,
            &missing.child_style,
            missing.token_budget,
        )
        .expect("action");
        missing.dispatch_hash =
            generic_child_dispatch_hash(&missing.identity, missing.action_digest)
                .expect("dispatch");
        assert!(matches!(
            children
                .reconcile_creation(missing)
                .expect("missing reconciliation"),
            ChildCreationEffectOutcome::Waiting { .. }
        ));
        assert_eq!(
            directory_count(&children.data, &sessions_root),
            initial_directory_count
        );
    }

    #[tokio::test]
    async fn production_string_task_preserves_typed_hash_and_reconciles_unquoted_prompt_once() {
        let task = serde_json::json!("inspect exact child recovery");
        let request = production_dispatch_request_for_task(task.clone()).await;
        let typed_task_bytes = serde_json::to_vec(&task).expect("typed task");
        assert_eq!(
            request.contract.task_hash,
            ContentHash::digest(&typed_task_bytes)
        );
        assert_eq!(
            request.action_digest,
            generic_child_action_digest(
                &request.identity,
                &request.contract,
                &request.child_style,
                request.token_budget,
            )
            .expect("action digest")
        );

        let temporary = tempdir().expect("temporary directory");
        let sessions_root = temporary.path().join("sessions");
        let workspace = temporary.path().join("workspace");
        let data = local_runtime_data_with_node_executors(
            RuntimeNodeExecutorData::native().expect("native executor registry"),
        );
        data.create_fixture_directory(CreateFixtureDirectoryDataRequest {
            directory: workspace.clone(),
            recursive: true,
        })
        .expect("workspace");
        let children = RuntimeChildGraphChildSessions::new(
            data,
            production_style_environment(),
            sessions_root.clone(),
            session_id(),
            sessions_root.join(session_id().to_string()),
            workspace.to_string_lossy().into_owned(),
            ChildMemoryAccess::None,
        );
        let ChildCreationEffectOutcome::Created(created) = children
            .create_after_dispatch(request.clone())
            .expect("create string-task child")
        else {
            panic!("created child receipt")
        };
        let child = SessionPersistenceLogic::new(local_runtime_data())
            .load_session(LoadSessionCommand {
                session_directory: sessions_root.join(created.child_session_id.to_string()),
                expected_session_id: created.child_session_id,
            })
            .expect("child replay");
        let origin = child.state.child_origin.expect("child origin");
        assert_eq!(origin.task, "inspect exact child recovery");
        assert_eq!(
            origin.input_hash,
            ContentHash::digest(b"inspect exact child recovery")
        );
        assert_ne!(origin.input_hash, request.contract.task_hash);

        let initial_directory_count = directory_count(&children.data, &sessions_root);
        assert_eq!(
            children
                .reconcile_creation(request)
                .expect("restart reconciliation"),
            ChildCreationEffectOutcome::Created(created)
        );
        assert_eq!(
            directory_count(&children.data, &sessions_root),
            initial_directory_count
        );
    }

    #[tokio::test]
    async fn multi_child_dispatch_is_stable_bounded_and_not_duplicated() {
        let (head, command) = initial_head();
        let journal = FakeJournal::new(head);
        let effects = FakeEffects::new();
        let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());

        let first = coordinator.coordinate(&command).await.expect("first turn");
        assert!(matches!(first, ChildGraphTurnResult::Waiting { .. }));
        let state = journal.canonical_state();
        assert_eq!(
            state
                .child_agents
                .values()
                .filter(|record| record.state == ChildAgentState::Active)
                .count(),
            2
        );
        assert_eq!(
            state
                .child_agents
                .values()
                .filter(|record| record.state == ChildAgentState::Proposed)
                .count(),
            1
        );
        assert_eq!(
            effects.state.lock().expect("effects").authorization_order,
            vec!["task-a", "task-b"]
        );

        coordinator.coordinate(&command).await.expect("recovery");
        let effects = effects.state.lock().expect("effects");
        assert_eq!(effects.create_calls.values().sum::<usize>(), 2);
        assert_eq!(effects.reconcile_calls.values().sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn create_crash_recovers_only_through_reconciliation() {
        let (head, mut command) = initial_head();
        command.outcome = ChildGraphNodeOutcome::Spawn {
            proposals: vec![proposal(&command.work, "task-a")],
        };
        command.maximum_in_flight = 1;
        let journal = FakeJournal::new(head);
        let effects = FakeEffects::new();
        effects
            .state
            .lock()
            .expect("effects")
            .create_after_effect_error_once = true;
        let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());

        assert!(matches!(
            coordinator.coordinate(&command).await,
            Err(ChildGraphTurnError::Effect(_))
        ));
        assert_eq!(
            journal
                .canonical_state()
                .child_agents
                .values()
                .next()
                .expect("child")
                .state,
            ChildAgentState::Dispatched
        );
        coordinator.coordinate(&command).await.expect("reconcile");
        let effects = effects.state.lock().expect("effects");
        assert_eq!(effects.create_calls.get("task-a"), Some(&1));
        assert_eq!(effects.reconcile_calls.get("task-a"), Some(&1));
    }

    #[tokio::test]
    async fn approval_waiting_and_denial_never_dispatch() {
        for outcome in [
            ChildCreationAuthorizationOutcome::Waiting {
                continuation_reference: String::from("approval-continuation"),
            },
            ChildCreationAuthorizationOutcome::Denied {
                code: String::from("user_denied"),
            },
        ] {
            let (head, mut command) = initial_head();
            command.outcome = ChildGraphNodeOutcome::Spawn {
                proposals: vec![proposal(&command.work, "task-a")],
            };
            let journal = FakeJournal::new(head);
            let effects = FakeEffects::new();
            effects.state.lock().expect("effects").authorization = outcome;
            let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());
            let result = coordinator
                .coordinate(&command)
                .await
                .expect("policy result");
            assert!(matches!(
                result,
                ChildGraphTurnResult::Waiting { .. } | ChildGraphTurnResult::Denied { .. }
            ));
            assert_eq!(
                journal
                    .canonical_state()
                    .child_agents
                    .values()
                    .next()
                    .expect("proposed")
                    .state,
                ChildAgentState::Proposed
            );
            assert!(
                effects
                    .state
                    .lock()
                    .expect("effects")
                    .create_calls
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn cas_conflicts_replan_without_duplicate_creation() {
        let (head, mut command) = initial_head();
        command.outcome = ChildGraphNodeOutcome::Spawn {
            proposals: vec![proposal(&command.work, "task-a")],
        };
        command.maximum_in_flight = 1;
        let journal = FakeJournal::new(head);
        journal.state.lock().expect("journal").conflicts_remaining = 4;
        let effects = FakeEffects::new();
        let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());

        coordinator
            .coordinate(&command)
            .await
            .expect("conflict recovery");
        assert_eq!(
            effects
                .state
                .lock()
                .expect("effects")
                .create_calls
                .get("task-a"),
            Some(&1)
        );
        let state = journal.canonical_state();
        assert_eq!(state.child_agents.len(), 1);
        assert_eq!(
            state.child_agents.values().next().expect("child").state,
            ChildAgentState::Active
        );
    }

    #[tokio::test]
    async fn accepted_cancellation_commits_outbox_and_exact_terminal_receipt_once() {
        let (head, spawn) = initial_head();
        let journal = FakeJournal::new(head);
        let effects = FakeEffects::new();
        let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());
        coordinator
            .coordinate(&spawn)
            .await
            .expect("create active children");
        let wait = switch_to_cancel_wait(&journal, spawn);

        assert!(matches!(
            coordinator.coordinate(&wait).await.expect("cancel children"),
            ChildGraphTurnResult::Failed { ref code, .. } if code == "parent_cancelled"
        ));
        let canonical = journal.canonical_state();
        let record = canonical
            .planner_worker
            .child_cancellations
            .values()
            .next()
            .expect("cancellation record");
        assert_eq!(record.state, GenericChildCancellationState::Completed);
        assert_eq!(
            record
                .receipt
                .as_ref()
                .expect("terminal receipt")
                .children
                .len(),
            2
        );
        coordinator
            .coordinate(&wait)
            .await
            .expect("completed replay");
        let effects = effects.state.lock().expect("effects");
        assert_eq!(effects.cancellation_dispatch_calls, 1);
        assert_eq!(effects.cancellation_reconcile_calls, 0);
    }

    #[tokio::test]
    async fn accepted_cancellation_every_append_cut_never_redispatches_ambiguity() {
        for cut in 1..=5 {
            let (head, spawn) = initial_head();
            let journal = FakeJournal::new(head);
            let effects = FakeEffects::new();
            let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());
            coordinator
                .coordinate(&spawn)
                .await
                .expect("create active children");
            let wait = switch_to_cancel_wait(&journal, spawn);
            {
                let mut state = journal.state.lock().expect("journal");
                state.crash_after_commit = Some(state.successful_appends + cut);
            }

            let _ = coordinator.coordinate(&wait).await;
            let recovered = coordinator
                .coordinate(&wait)
                .await
                .expect("recover cancellation cut");
            let canonical = journal.canonical_state();
            let record = canonical
                .planner_worker
                .child_cancellations
                .values()
                .next()
                .expect("cancellation record");
            let effects = effects.state.lock().expect("effects");
            assert!(effects.cancellation_dispatch_calls <= 1, "cut {cut}");
            if cut == 4 {
                assert!(matches!(recovered, ChildGraphTurnResult::Ambiguous { .. }));
                assert_eq!(effects.cancellation_dispatch_calls, 0);
                assert_eq!(effects.cancellation_reconcile_calls, 1);
                assert_eq!(record.state, GenericChildCancellationState::Ambiguous);
            } else {
                assert!(matches!(recovered, ChildGraphTurnResult::Failed { .. }));
                assert_eq!(effects.cancellation_dispatch_calls, 1);
                assert_eq!(record.state, GenericChildCancellationState::Completed);
            }
        }
    }

    #[tokio::test]
    async fn every_canonical_append_crash_cut_recovers_without_redispatch() {
        for cut in 1..=4 {
            let (head, mut command) = initial_head();
            command.outcome = ChildGraphNodeOutcome::Spawn {
                proposals: vec![proposal(&command.work, "task-a")],
            };
            command.maximum_in_flight = 1;
            let journal = FakeJournal::new(head);
            journal.state.lock().expect("journal").crash_after_commit = Some(cut);
            let effects = FakeEffects::new();
            let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());

            let _ = coordinator.coordinate(&command).await;
            coordinator
                .coordinate(&command)
                .await
                .expect("recover append cut");
            let effects = effects.state.lock().expect("effects");
            assert!(effects.create_calls.values().sum::<usize>() <= 1);
            assert_eq!(
                journal
                    .canonical_state()
                    .child_agents
                    .values()
                    .next()
                    .expect("child")
                    .state,
                ChildAgentState::Active
            );
        }
    }

    #[tokio::test]
    async fn spawn_stops_at_active_and_replay_does_not_observe_terminal_children() {
        let (head, mut command) = initial_head();
        command.maximum_in_flight = 3;
        let journal = FakeJournal::new(head);
        let effects = FakeEffects::new();
        effects.state.lock().expect("effects").terminals = BTreeSet::from([
            String::from("task-a"),
            String::from("task-b"),
            String::from("task-c"),
        ]);
        let coordinator = ChildGraphTurnCoordinator::new(journal.clone(), effects.clone());

        let result = coordinator.coordinate(&command).await.expect("complete");
        assert!(matches!(result, ChildGraphTurnResult::Applied { .. }));
        let first_types = journal
            .state
            .lock()
            .expect("journal")
            .appended_types
            .clone();
        let result = coordinator
            .coordinate(&command)
            .await
            .expect("idempotent replay");
        assert_eq!(result, ChildGraphTurnResult::Applied { appended_events: 0 });
        assert_eq!(
            journal.state.lock().expect("journal").appended_types,
            first_types
        );
        assert!(
            journal
                .canonical_state()
                .child_agents
                .values()
                .all(|record| {
                    record.state == ChildAgentState::Active && record.terminal_receipt.is_none()
                })
        );
    }

    #[tokio::test]
    async fn executor_work_and_configuration_substitution_fail_before_effects() {
        let (head, command) = initial_head();
        for mut substituted in [
            {
                let mut value = command.clone();
                value.executor.executor_version = String::from("2.0.0");
                value
            },
            {
                let mut value = command.clone();
                value.work.attempt = 2;
                value
            },
            {
                let mut value = command.clone();
                value.configuration_hash = ContentHash::digest(b"substituted");
                value
            },
        ] {
            let journal = FakeJournal::new(head.clone());
            let effects = FakeEffects::new();
            let coordinator = ChildGraphTurnCoordinator::new(journal, effects.clone());
            assert!(coordinator.coordinate(&substituted).await.is_err());
            let effects = effects.state.lock().expect("effects");
            assert!(effects.authorization_order.is_empty());
            assert!(effects.create_calls.is_empty());
            substituted.maximum_in_flight = 1;
        }
    }

    #[tokio::test]
    async fn canonical_reviewer_receipt_rejects_work_plan_and_configuration_substitution() {
        let work = NodeWorkIdentity {
            run_id: String::from("review-run"),
            node_id: String::from("quality-gate"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 3,
        };
        let routing = ReviewRoutingProposal {
            disposition: ReviewDisposition::Approved,
            destination_node_id: String::from("accepted"),
            current_revision: 0,
            next_revision: None,
            rejected_task_ids: Vec::new(),
            findings: Vec::new(),
            evidence_hash: ContentHash::digest(b"review-evidence"),
        };
        let execution_plan_hash = ContentHash::digest(b"review-plan");
        let configuration_hash = ContentHash::digest(b"review-configuration");
        let reviewer = CanonicalReceiptChildGraphReviewer {
            receipt: Some(CanonicalChildGraphReviewerReceipt {
                work: work.clone(),
                execution_plan_hash,
                configuration_hash,
                routing: routing.clone(),
                provider_result_hash: ContentHash::digest(b"provider-receipt"),
            }),
        };
        let request = ChildReviewEvidenceRequest {
            session_id: session_id(),
            work,
            execution_plan_hash,
            configuration_hash,
            routing,
        };
        let command = ChildGraphReviewerCommand {
            request,
            request_hash: ContentHash::digest(b"review-request"),
            idempotency_key: ContentHash::digest(b"review-idempotency"),
            phase: ChildGraphAncillaryApplicationPhase::Initial,
        };
        assert!(matches!(
            reviewer
                .review(command.clone())
                .await
                .expect("exact receipt"),
            ChildGraphReviewerOutcome::Completed { .. }
        ));
        let mut substituted = command;
        substituted.request.configuration_hash = ContentHash::digest(b"substituted");
        assert!(matches!(
            reviewer
                .review(substituted)
                .await
                .expect("substitution denial"),
            ChildGraphReviewerOutcome::Denied { code }
                if code == "child_graph_reviewer_receipt_substitution"
        ));
    }
}
