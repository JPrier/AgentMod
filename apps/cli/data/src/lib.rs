//! CLI business dataset construction.
#![allow(missing_docs, reason = "data-local turn records are boundary-specific")]
#![allow(
    clippy::missing_errors_doc,
    reason = "the CLI data port uses one documented closed error taxonomy"
)]

use agentmod_cli_dependency::{
    CliDependencyPort, DependencyBranchSessionRequest, DependencyCancelTurnRequest,
    DependencyCreateDeferredTurnRequest, DependencyCreateSessionRequest,
    DependencyInspectSessionRequest, DependencyListSessionsRequest,
    DependencyResolveApprovalRequest, DependencyRunTurnRequest, DependencyRunTurnStream,
    DependencyRunTurnStreamItem, DependencyRuntimeAvailability, DependencyRuntimeHealthRequest,
    DependencySchedule, DependencySchedulePayload, DependencyScheduleTrigger,
    DependencyScheduledExecution, DependencyScheduledRun, DependencySubscribeSessionRequest,
    DependencyTurnEvent,
};
use agentmod_primitives::{CancellationId, Sequence, SessionId};
use serde_json::Value;
use thiserror::Error;

/// Data-owned request for the runtime portion of a doctor report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealthDataRequest {
    /// Logical endpoint label selected by validated CLI configuration.
    pub endpoint_label: String,
}

/// Data-owned normalized runtime health record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealthDataRecord {
    /// Availability normalized independently of the runtime wire contract.
    pub availability: RuntimeHealthDataAvailability,
    /// Safe runtime build version.
    pub version: String,
}

/// Data-owned runtime availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeHealthDataAvailability {
    /// Runtime dependencies are ready.
    Ready,
    /// Runtime answered in a degraded state.
    Degraded,
    /// Runtime is unavailable.
    Unavailable,
}

/// Data-owned create request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionDataRequest {
    /// Workspace.
    pub workspace: String,
    /// Explicit style.
    pub style: String,
}

/// Data-owned create result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionDataRecord {
    /// Runtime session ID.
    pub session_id: SessionId,
}

/// Data-owned list request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListSessionsDataRequest {
    /// Maximum rows.
    pub limit: u32,
}

/// Data-owned session row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionSummaryDataRecord {
    /// Session ID.
    pub id: SessionId,
    /// Workspace label.
    pub workspace_label: String,
    /// Style.
    pub style: String,
    /// Last sequence.
    pub sequence: Sequence,
    /// Lifecycle.
    pub state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectSessionDataRequest {
    pub session_id: SessionId,
    pub at: Option<Sequence>,
    pub replay: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InspectSessionDataRecord {
    pub session_id: SessionId,
    pub head_sequence: Sequence,
    pub inspected_sequence: Sequence,
    pub event_count: u64,
    pub state: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeSessionDataRequest {
    pub session_id: SessionId,
    pub after: Option<Sequence>,
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventDataRecord {
    pub sequence: Sequence,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventPageDataRecord {
    pub events: Vec<SessionEventDataRecord>,
    pub head_sequence: Sequence,
    pub last_delivered_sequence: Option<Sequence>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionDataRequest {
    pub session_id: SessionId,
    pub at: Sequence,
    pub style: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionDataRecord {
    pub session_id: SessionId,
    pub parent_session_id: SessionId,
    pub fork_sequence: Sequence,
    pub child_head_sequence: Sequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleDataTrigger {
    AtMillis(i64),
    Interval {
        starts_at_ms: i64,
        every_ms: u64,
    },
    RuntimeEvent {
        event_type: String,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleDataPayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleDataRecord {
    pub schedule_id: String,
    pub session_id: SessionId,
    pub idempotency_id: String,
    pub style: String,
    pub workspace: String,
    pub permission_policy: String,
    pub provider: String,
    pub model: String,
    pub token_budget: u64,
    pub cost_budget_micros: u64,
    pub trigger: ScheduleDataTrigger,
    pub payload: ScheduleDataPayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledExecutionDataRecord {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub claimed_at_ms: i64,
    pub schedule: ScheduleDataRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleStoreDataRecord {
    pub schedule_id: String,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateDeferredTurnDataRequest {
    pub session_id: SessionId,
    pub continuation_id: String,
    pub schedule_id: String,
    pub prompt: String,
    pub workspace: String,
    pub provider: String,
    pub model: String,
    pub options: Value,
    pub style: String,
    pub cancellation_id: CancellationId,
    pub trigger: ScheduleDataTrigger,
    pub expires_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledRunDataRecord {
    pub execution_id: String,
    pub schedule_id: String,
    pub terminal: bool,
    pub succeeded: bool,
    pub last_committed_sequence: Option<Sequence>,
    pub awaiting_continuation: Option<String>,
    pub error: Option<String>,
}

/// Data-owned durable turn request.
#[derive(Clone, Debug, PartialEq)]
pub struct RunTurnDataRequest {
    pub session_id: SessionId,
    pub prompt: String,
    pub provider: String,
    pub model: String,
    pub options: Value,
    pub cancellation_id: Option<CancellationId>,
}

/// Data-owned active-turn cancellation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelTurnDataRequest {
    pub cancellation_id: CancellationId,
    pub reason: String,
}

/// Data-owned provider lifecycle record.
#[derive(Clone, Debug, PartialEq)]
pub enum TurnDataEvent {
    Started,
    Text(String),
    ToolDelta {
        call_id: String,
        name: String,
        arguments: String,
    },
    ToolProposed {
        continuation_id: String,
        call_id: String,
        tool: String,
        arguments: Value,
    },
    Completed {
        reason: String,
        input_tokens: u64,
        output_tokens: u64,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

/// Data-owned normalized turn record.
#[derive(Clone, Debug, PartialEq)]
pub struct RunTurnDataRecord {
    pub events: Vec<TurnDataEvent>,
    pub first_committed_sequence: Sequence,
    pub last_committed_sequence: Sequence,
    pub awaiting_continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RunTurnDataStreamItem {
    Event {
        event: TurnDataEvent,
        committed_sequence: Sequence,
    },
    Complete {
        first_committed_sequence: Sequence,
        last_committed_sequence: Sequence,
        awaiting_continuation: Option<String>,
    },
}

pub struct RunTurnDataStream {
    dependency: DependencyRunTurnStream,
}

impl RunTurnDataStream {
    #[must_use]
    pub fn next(&self) -> Option<Result<RunTurnDataStreamItem, DataError>> {
        self.dependency.next().map(|result| {
            result
                .map(|item| match item {
                    DependencyRunTurnStreamItem::Event {
                        event,
                        committed_sequence,
                    } => RunTurnDataStreamItem::Event {
                        event: map_turn_event(event),
                        committed_sequence,
                    },
                    DependencyRunTurnStreamItem::Complete {
                        first_committed_sequence,
                        last_committed_sequence,
                        awaiting_continuation,
                    } => RunTurnDataStreamItem::Complete {
                        first_committed_sequence,
                        last_committed_sequence,
                        awaiting_continuation,
                    },
                })
                .map_err(|_| DataError::RuntimeClient {
                    detail: String::from("run turn stream failed"),
                })
        })
    }
}

/// Data-owned durable approval request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveApprovalDataRequest {
    pub session_id: SessionId,
    pub continuation_id: String,
    pub approved: bool,
}

/// Data-owned durable approval result.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolveApprovalDataRecord {
    pub transitioned: bool,
    pub events: Vec<TurnDataEvent>,
    pub last_committed_sequence: Option<Sequence>,
    pub awaiting_continuation: Option<String>,
}

/// Narrow CLI data interface consumed only by CLI logic.
pub trait CliDataPort {
    /// Builds the normalized runtime health dataset.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the request is invalid or the selected
    /// dependency cannot construct the runtime dataset.
    fn runtime_health(
        &self,
        request: RuntimeHealthDataRequest,
    ) -> Result<RuntimeHealthDataRecord, DataError>;

    /// Creates a durable session through the selected runtime dependency.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the dependency fails.
    fn create_session(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<CreateSessionDataRecord, DataError>;

    /// Lists bounded session metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the dependency fails.
    fn list_sessions(
        &self,
        request: ListSessionsDataRequest,
    ) -> Result<Vec<SessionSummaryDataRecord>, DataError>;

    /// Builds pure replay state.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the runtime dependency fails.
    fn inspect_session(
        &self,
        request: InspectSessionDataRequest,
    ) -> Result<InspectSessionDataRecord, DataError>;

    /// Builds one verified bounded reconnect page.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the selected runtime dependency fails.
    fn subscribe_session(
        &self,
        request: SubscribeSessionDataRequest,
    ) -> Result<SessionEventPageDataRecord, DataError>;

    /// Builds an atomic branch result.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the runtime dependency fails.
    fn branch_session(
        &self,
        request: BranchSessionDataRequest,
    ) -> Result<BranchSessionDataRecord, DataError>;

    /// Builds one durable turn dataset.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the runtime dependency fails.
    fn run_turn(&self, request: RunTurnDataRequest) -> Result<RunTurnDataRecord, DataError>;

    /// Starts a bounded turn stream.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the runtime dependency cannot start the stream.
    fn run_turn_stream(&self, request: RunTurnDataRequest) -> Result<RunTurnDataStream, DataError>;

    /// Cancels one active runtime turn.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the runtime dependency fails.
    fn cancel_turn(&self, request: CancelTurnDataRequest) -> Result<(), DataError>;

    /// Resolves and resumes a durable approval dataset.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the runtime dependency fails.
    fn resolve_approval(
        &self,
        request: ResolveApprovalDataRequest,
    ) -> Result<ResolveApprovalDataRecord, DataError>;

    fn upsert_schedule(
        &self,
        _schedule: ScheduleDataRecord,
    ) -> Result<ScheduleStoreDataRecord, DataError> {
        Err(schedule_unavailable())
    }

    fn create_deferred_turn(
        &self,
        _request: CreateDeferredTurnDataRequest,
    ) -> Result<(), DataError> {
        Err(schedule_unavailable())
    }

    fn remove_schedule(&self, _schedule_id: &str) -> Result<bool, DataError> {
        Err(schedule_unavailable())
    }

    fn list_schedules(&self, _limit: u32) -> Result<Vec<ScheduleDataRecord>, DataError> {
        Err(schedule_unavailable())
    }

    fn claim_due_schedules(
        &self,
        _limit: u32,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, DataError> {
        Err(schedule_unavailable())
    }

    fn complete_scheduled_execution(
        &self,
        _execution_id: &str,
        _succeeded: bool,
    ) -> Result<bool, DataError> {
        Err(schedule_unavailable())
    }

    fn run_due_schedules(&self, _limit: u32) -> Result<Vec<ScheduledRunDataRecord>, DataError> {
        Err(schedule_unavailable())
    }
}

/// CLI data implementation over an injected runtime client dependency.
#[derive(Clone, Debug)]
pub struct CliData<D> {
    dependency: D,
}

impl<D> CliData<D> {
    /// Creates CLI data with a concrete dependency implementation.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D> CliDataPort for CliData<D>
where
    D: CliDependencyPort,
{
    fn runtime_health(
        &self,
        request: RuntimeHealthDataRequest,
    ) -> Result<RuntimeHealthDataRecord, DataError> {
        if request.endpoint_label.trim().is_empty() {
            return Err(DataError::InvalidEndpointLabel);
        }
        let response = self
            .dependency
            .runtime_health(DependencyRuntimeHealthRequest {
                client_label: request.endpoint_label,
            })
            .map_err(|error| DataError::RuntimeClient {
                detail: error.to_string(),
            })?;
        Ok(RuntimeHealthDataRecord {
            availability: match response.availability {
                DependencyRuntimeAvailability::Ready => RuntimeHealthDataAvailability::Ready,
                DependencyRuntimeAvailability::Degraded => RuntimeHealthDataAvailability::Degraded,
                DependencyRuntimeAvailability::Unavailable => {
                    RuntimeHealthDataAvailability::Unavailable
                }
            },
            version: response.runtime_version,
        })
    }

    fn create_session(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<CreateSessionDataRecord, DataError> {
        self.dependency
            .create_session(DependencyCreateSessionRequest {
                workspace: request.workspace,
                style: request.style,
            })
            .map(|response| CreateSessionDataRecord {
                session_id: response.session_id,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("create session failed"),
            })
    }

    fn list_sessions(
        &self,
        request: ListSessionsDataRequest,
    ) -> Result<Vec<SessionSummaryDataRecord>, DataError> {
        self.dependency
            .list_sessions(DependencyListSessionsRequest {
                limit: request.limit,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("list sessions failed"),
            })
            .map(|sessions| {
                sessions
                    .into_iter()
                    .map(|session| SessionSummaryDataRecord {
                        id: session.id,
                        workspace_label: session.workspace_label,
                        style: session.style,
                        sequence: session.sequence,
                        state: session.state,
                    })
                    .collect()
            })
    }

    fn inspect_session(
        &self,
        request: InspectSessionDataRequest,
    ) -> Result<InspectSessionDataRecord, DataError> {
        self.dependency
            .inspect_session(DependencyInspectSessionRequest {
                session_id: request.session_id,
                at: request.at,
                replay: request.replay,
            })
            .map(|record| InspectSessionDataRecord {
                session_id: record.session_id,
                head_sequence: record.head_sequence,
                inspected_sequence: record.inspected_sequence,
                event_count: record.event_count,
                state: record.state,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("inspect session failed"),
            })
    }

    fn subscribe_session(
        &self,
        request: SubscribeSessionDataRequest,
    ) -> Result<SessionEventPageDataRecord, DataError> {
        self.dependency
            .subscribe_session(DependencySubscribeSessionRequest {
                session_id: request.session_id,
                after: request.after,
                limit: request.limit,
            })
            .map(|record| SessionEventPageDataRecord {
                events: record
                    .events
                    .into_iter()
                    .map(|event| SessionEventDataRecord {
                        sequence: event.sequence,
                        event_type: event.event_type,
                        payload: event.payload,
                    })
                    .collect(),
                head_sequence: record.head_sequence,
                last_delivered_sequence: record.last_delivered_sequence,
                has_more: record.has_more,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("subscribe session failed"),
            })
    }

    fn branch_session(
        &self,
        request: BranchSessionDataRequest,
    ) -> Result<BranchSessionDataRecord, DataError> {
        self.dependency
            .branch_session(DependencyBranchSessionRequest {
                session_id: request.session_id,
                at: request.at,
                style: request.style,
            })
            .map(|record| BranchSessionDataRecord {
                session_id: record.session_id,
                parent_session_id: record.parent_session_id,
                fork_sequence: record.fork_sequence,
                child_head_sequence: record.child_head_sequence,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("branch session failed"),
            })
    }

    fn run_turn(&self, request: RunTurnDataRequest) -> Result<RunTurnDataRecord, DataError> {
        self.dependency
            .run_turn(DependencyRunTurnRequest {
                session_id: request.session_id,
                prompt: request.prompt,
                provider: request.provider,
                model: request.model,
                options: request.options,
                cancellation_id: request.cancellation_id,
            })
            .map(|response| RunTurnDataRecord {
                events: response.events.into_iter().map(map_turn_event).collect(),
                first_committed_sequence: response.first_committed_sequence,
                last_committed_sequence: response.last_committed_sequence,
                awaiting_continuation: response.awaiting_continuation,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("run turn failed"),
            })
    }

    fn run_turn_stream(&self, request: RunTurnDataRequest) -> Result<RunTurnDataStream, DataError> {
        self.dependency
            .run_turn_stream(DependencyRunTurnRequest {
                session_id: request.session_id,
                prompt: request.prompt,
                provider: request.provider,
                model: request.model,
                options: request.options,
                cancellation_id: request.cancellation_id,
            })
            .map(|dependency| RunTurnDataStream { dependency })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("run turn stream failed"),
            })
    }

    fn cancel_turn(&self, request: CancelTurnDataRequest) -> Result<(), DataError> {
        self.dependency
            .cancel_turn(DependencyCancelTurnRequest {
                cancellation_id: request.cancellation_id,
                reason: request.reason,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("cancel turn failed"),
            })
    }

    fn resolve_approval(
        &self,
        request: ResolveApprovalDataRequest,
    ) -> Result<ResolveApprovalDataRecord, DataError> {
        self.dependency
            .resolve_approval(DependencyResolveApprovalRequest {
                session_id: request.session_id,
                continuation_id: request.continuation_id,
                approved: request.approved,
            })
            .map(|response| ResolveApprovalDataRecord {
                transitioned: response.transitioned,
                events: response.events.into_iter().map(map_turn_event).collect(),
                last_committed_sequence: response.last_committed_sequence,
                awaiting_continuation: response.awaiting_continuation,
            })
            .map_err(|_| DataError::RuntimeClient {
                detail: String::from("resolve approval failed"),
            })
    }

    fn upsert_schedule(
        &self,
        schedule: ScheduleDataRecord,
    ) -> Result<ScheduleStoreDataRecord, DataError> {
        self.dependency
            .upsert_schedule(to_dependency_schedule(schedule))
            .map(|value| ScheduleStoreDataRecord {
                schedule_id: value.schedule_id,
                replayed: value.replayed,
            })
            .map_err(|error| runtime_error(&error))
    }

    fn create_deferred_turn(
        &self,
        request: CreateDeferredTurnDataRequest,
    ) -> Result<(), DataError> {
        self.dependency
            .create_deferred_turn(DependencyCreateDeferredTurnRequest {
                session_id: request.session_id,
                continuation_id: request.continuation_id,
                schedule_id: request.schedule_id,
                prompt: request.prompt,
                workspace: request.workspace,
                provider: request.provider,
                model: request.model,
                options: request.options,
                style: request.style,
                cancellation_id: request.cancellation_id,
                trigger: to_dependency_trigger(request.trigger),
                expires_at_ms: request.expires_at_ms,
            })
            .map_err(|error| runtime_error(&error))
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, DataError> {
        self.dependency
            .remove_schedule(schedule_id)
            .map_err(|error| runtime_error(&error))
    }

    fn list_schedules(&self, limit: u32) -> Result<Vec<ScheduleDataRecord>, DataError> {
        self.dependency
            .list_schedules(limit)
            .map(|values| values.into_iter().map(from_dependency_schedule).collect())
            .map_err(|error| runtime_error(&error))
    }

    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, DataError> {
        self.dependency
            .claim_due_schedules(limit)
            .map(|values| values.into_iter().map(from_dependency_execution).collect())
            .map_err(|error| runtime_error(&error))
    }

    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, DataError> {
        self.dependency
            .complete_scheduled_execution(execution_id, succeeded)
            .map_err(|error| runtime_error(&error))
    }

    fn run_due_schedules(&self, limit: u32) -> Result<Vec<ScheduledRunDataRecord>, DataError> {
        self.dependency
            .run_due_schedules(limit)
            .map(|values| values.into_iter().map(from_dependency_run).collect())
            .map_err(|error| runtime_error(&error))
    }
}

fn schedule_unavailable() -> DataError {
    DataError::RuntimeClient {
        detail: String::from("schedule data unavailable"),
    }
}

fn runtime_error(error: &agentmod_cli_dependency::DependencyError) -> DataError {
    DataError::RuntimeClient {
        detail: error.to_string(),
    }
}

fn to_dependency_schedule(value: ScheduleDataRecord) -> DependencySchedule {
    DependencySchedule {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: to_dependency_trigger(value.trigger),
        payload: match value.payload {
            ScheduleDataPayload::Prompt { prompt } => DependencySchedulePayload::Prompt { prompt },
            ScheduleDataPayload::Continuation { continuation_id } => {
                DependencySchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn to_dependency_trigger(value: ScheduleDataTrigger) -> DependencyScheduleTrigger {
    match value {
        ScheduleDataTrigger::AtMillis(value) => DependencyScheduleTrigger::AtMillis(value),
        ScheduleDataTrigger::Interval {
            starts_at_ms,
            every_ms,
        } => DependencyScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        },
        ScheduleDataTrigger::RuntimeEvent { event_type } => {
            DependencyScheduleTrigger::RuntimeEvent { event_type }
        }
        ScheduleDataTrigger::ProcessOutput {
            process_id,
            contains,
        } => DependencyScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        },
    }
}

fn from_dependency_schedule(value: DependencySchedule) -> ScheduleDataRecord {
    ScheduleDataRecord {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            DependencyScheduleTrigger::AtMillis(value) => ScheduleDataTrigger::AtMillis(value),
            DependencyScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleDataTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            DependencyScheduleTrigger::RuntimeEvent { event_type } => {
                ScheduleDataTrigger::RuntimeEvent { event_type }
            }
            DependencyScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleDataTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            DependencySchedulePayload::Prompt { prompt } => ScheduleDataPayload::Prompt { prompt },
            DependencySchedulePayload::Continuation { continuation_id } => {
                ScheduleDataPayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_dependency_execution(value: DependencyScheduledExecution) -> ScheduledExecutionDataRecord {
    ScheduledExecutionDataRecord {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        claimed_at_ms: value.claimed_at_ms,
        schedule: from_dependency_schedule(value.schedule),
    }
}

fn from_dependency_run(value: DependencyScheduledRun) -> ScheduledRunDataRecord {
    ScheduledRunDataRecord {
        execution_id: value.execution_id,
        schedule_id: value.schedule_id,
        terminal: value.terminal,
        succeeded: value.succeeded,
        last_committed_sequence: value.last_committed_sequence,
        awaiting_continuation: value.awaiting_continuation,
        error: value.error,
    }
}

fn map_turn_event(event: DependencyTurnEvent) -> TurnDataEvent {
    match event {
        DependencyTurnEvent::Started => TurnDataEvent::Started,
        DependencyTurnEvent::Text(value) => TurnDataEvent::Text(value),
        DependencyTurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => TurnDataEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        DependencyTurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => TurnDataEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        DependencyTurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        } => TurnDataEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        },
        DependencyTurnEvent::Cancelled => TurnDataEvent::Cancelled,
        DependencyTurnEvent::Failed {
            code,
            message,
            retryable,
        } => TurnDataEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

/// CLI data-layer failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DataError {
    /// CLI configuration did not identify a runtime endpoint.
    #[error("runtime endpoint label is empty")]
    InvalidEndpointLabel,
    /// The selected runtime client could not construct the requested dataset.
    #[error("runtime client failed: {detail}")]
    RuntimeClient {
        /// Sanitized dependency diagnostic.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_cli_dependency::{
        DependencyBranchSessionResponse, DependencyCreateSessionResponse, DependencyError,
        DependencyInspectSessionResponse, DependencyResolveApprovalResponse,
        DependencyRunTurnResponse, DependencyRuntimeHealthResponse, DependencySessionEventPage,
        DependencySessionSummary,
    };
    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct MockDependency {
        observed: RefCell<Vec<DependencyRuntimeHealthRequest>>,
    }

    impl CliDependencyPort for MockDependency {
        fn runtime_health(
            &self,
            request: DependencyRuntimeHealthRequest,
        ) -> Result<DependencyRuntimeHealthResponse, DependencyError> {
            self.observed.borrow_mut().push(request);
            Ok(DependencyRuntimeHealthResponse {
                availability: DependencyRuntimeAvailability::Degraded,
                runtime_version: "9.8.7".into(),
            })
        }

        fn create_session(
            &self,
            _request: DependencyCreateSessionRequest,
        ) -> Result<DependencyCreateSessionResponse, DependencyError> {
            Ok(DependencyCreateSessionResponse {
                session_id: SessionId::from_uuid(Uuid::from_u128(1)),
            })
        }

        fn list_sessions(
            &self,
            _request: DependencyListSessionsRequest,
        ) -> Result<Vec<DependencySessionSummary>, DependencyError> {
            Ok(vec![])
        }

        fn inspect_session(
            &self,
            request: DependencyInspectSessionRequest,
        ) -> Result<DependencyInspectSessionResponse, DependencyError> {
            Ok(DependencyInspectSessionResponse {
                session_id: request.session_id,
                head_sequence: Sequence::FIRST,
                inspected_sequence: Sequence::FIRST,
                event_count: 1,
                state: serde_json::json!({"fixture": true}),
            })
        }

        fn subscribe_session(
            &self,
            request: DependencySubscribeSessionRequest,
        ) -> Result<DependencySessionEventPage, DependencyError> {
            Ok(DependencySessionEventPage {
                events: vec![],
                head_sequence: Sequence::FIRST,
                last_delivered_sequence: request.after,
                has_more: false,
            })
        }

        fn branch_session(
            &self,
            request: DependencyBranchSessionRequest,
        ) -> Result<DependencyBranchSessionResponse, DependencyError> {
            Ok(DependencyBranchSessionResponse {
                session_id: SessionId::from_uuid(Uuid::from_u128(2)),
                parent_session_id: request.session_id,
                fork_sequence: request.at,
                child_head_sequence: Sequence::new(2).expect("sequence"),
            })
        }

        fn run_turn(
            &self,
            _request: DependencyRunTurnRequest,
        ) -> Result<DependencyRunTurnResponse, DependencyError> {
            Ok(DependencyRunTurnResponse {
                events: vec![],
                first_committed_sequence: Sequence::FIRST,
                last_committed_sequence: Sequence::FIRST,
                awaiting_continuation: None,
            })
        }

        fn run_turn_stream(
            &self,
            _request: DependencyRunTurnRequest,
        ) -> Result<DependencyRunTurnStream, DependencyError> {
            Err(DependencyError::UnsupportedRuntimeRequest)
        }

        fn cancel_turn(
            &self,
            _request: DependencyCancelTurnRequest,
        ) -> Result<(), DependencyError> {
            Ok(())
        }

        fn resolve_approval(
            &self,
            _request: DependencyResolveApprovalRequest,
        ) -> Result<DependencyResolveApprovalResponse, DependencyError> {
            Ok(DependencyResolveApprovalResponse {
                transitioned: true,
                events: vec![],
                last_committed_sequence: Some(Sequence::FIRST),
                awaiting_continuation: None,
            })
        }
    }

    #[test]
    fn maps_dependency_health_into_data_record() {
        let data = CliData::new(MockDependency::default());
        assert_eq!(
            data.runtime_health(RuntimeHealthDataRequest {
                endpoint_label: "local-runtime".into(),
            })
            .expect("health dataset"),
            RuntimeHealthDataRecord {
                availability: RuntimeHealthDataAvailability::Degraded,
                version: "9.8.7".into(),
            }
        );
        assert_eq!(
            data.dependency.observed.into_inner(),
            vec![DependencyRuntimeHealthRequest {
                client_label: "local-runtime".into(),
            }]
        );
    }

    #[test]
    fn invalid_endpoint_is_rejected_without_calling_dependency() {
        let data = CliData::new(MockDependency::default());
        assert_eq!(
            data.runtime_health(RuntimeHealthDataRequest {
                endpoint_label: " ".into(),
            }),
            Err(DataError::InvalidEndpointLabel)
        );
        assert!(data.dependency.observed.into_inner().is_empty());
    }
}
