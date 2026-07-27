//! CLI business behavior.
#![allow(
    missing_docs,
    reason = "logic-local turn records are boundary-specific"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the CLI logic port uses one documented closed error taxonomy"
)]

use agentmod_cli_data::{
    BranchSessionDataRequest, CancelTurnDataRequest, CliDataPort, CreateSessionDataRequest,
    InspectSessionDataRequest, ListSessionsDataRequest, ResolveApprovalDataRequest,
    RunTurnDataRequest, RunTurnDataStream, RunTurnDataStreamItem, RuntimeHealthDataAvailability,
    RuntimeHealthDataRequest, ScheduleDataPayload, ScheduleDataRecord, ScheduleDataTrigger,
    ScheduledExecutionDataRecord, ScheduledRunDataRecord, SubscribeSessionDataRequest,
    TurnDataEvent,
};
use agentmod_primitives::{CancellationId, Sequence, SessionId};
use serde_json::Value;
use thiserror::Error;

/// Logic-owned doctor command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunDoctorCommand {
    /// Business policy requiring every check to be ready.
    pub strict: bool,
    /// Selected runtime endpoint label.
    pub runtime_endpoint: String,
}

/// Logic-owned doctor result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DoctorResult {
    /// Overall business state.
    pub state: DoctorState,
    /// Whether endpoint policy considers the invocation successful.
    pub successful: bool,
    /// Completed checks.
    pub checks: Vec<DoctorCheck>,
}

/// Overall doctor state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DoctorState {
    /// All required checks are ready.
    Ready,
    /// At least one check is degraded but reported.
    Degraded,
    /// At least one check is unavailable.
    Unavailable,
}

/// Logic-owned completed check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DoctorCheck {
    /// Stable check name.
    pub name: String,
    /// Check state.
    pub state: DoctorState,
    /// Redacted user-facing detail.
    pub detail: String,
}

/// Logic-owned create command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionCommand {
    /// Workspace.
    pub workspace: String,
    /// Explicit style.
    pub style: String,
}

/// Logic-owned create result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionResult {
    /// Runtime session ID.
    pub session_id: SessionId,
}

/// Logic-owned list command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListSessionsCommand {
    /// Maximum rows.
    pub limit: u32,
}

/// Logic-owned session row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionSummaryResult {
    /// Session ID.
    pub id: SessionId,
    /// Workspace label.
    pub workspace_label: String,
    /// Explicit style.
    pub style: String,
    /// Last sequence.
    pub sequence: Sequence,
    /// Lifecycle.
    pub state: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InspectSessionCommand {
    pub session_id: SessionId,
    pub at: Option<Sequence>,
    pub replay: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InspectSessionResult {
    pub session_id: SessionId,
    pub head_sequence: Sequence,
    pub inspected_sequence: Sequence,
    pub event_count: u64,
    pub state: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeSessionCommand {
    pub session_id: SessionId,
    pub after: Option<Sequence>,
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventResult {
    pub sequence: Sequence,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventPageResult {
    pub events: Vec<SessionEventResult>,
    pub head_sequence: Sequence,
    pub last_delivered_sequence: Option<Sequence>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionCommand {
    pub session_id: SessionId,
    pub at: Sequence,
    pub style: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionResult {
    pub session_id: SessionId,
    pub parent_session_id: SessionId,
    pub fork_sequence: Sequence,
    pub child_head_sequence: Sequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleTrigger {
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
pub enum SchedulePayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleCommand {
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
    pub trigger: ScheduleTrigger,
    pub payload: SchedulePayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleResult {
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
    pub trigger: ScheduleTrigger,
    pub payload: SchedulePayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledExecutionResult {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub schedule: ScheduleResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleStoreResult {
    pub schedule_id: String,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledRunResult {
    pub execution_id: String,
    pub schedule_id: String,
    pub terminal: bool,
    pub succeeded: bool,
    pub last_committed_sequence: Option<Sequence>,
    pub awaiting_continuation: Option<String>,
    pub error: Option<String>,
}

/// Logic-owned durable turn command.
#[derive(Clone, Debug, PartialEq)]
pub struct RunTurnCommand {
    pub session_id: SessionId,
    pub prompt: String,
    pub provider: String,
    pub model: String,
    pub options: Value,
    pub cancellation_id: Option<CancellationId>,
}

/// Logic-owned active-turn cancellation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelTurnCommand {
    pub cancellation_id: CancellationId,
    pub reason: String,
}

/// Logic-owned provider event.
#[derive(Clone, Debug, PartialEq)]
pub enum TurnEvent {
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

/// Logic-owned turn result.
#[derive(Clone, Debug, PartialEq)]
pub struct RunTurnResult {
    pub events: Vec<TurnEvent>,
    pub first_committed_sequence: Sequence,
    pub last_committed_sequence: Sequence,
    pub awaiting_continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RunTurnStreamItem {
    Event {
        event: TurnEvent,
        committed_sequence: Sequence,
    },
    Complete {
        first_committed_sequence: Sequence,
        last_committed_sequence: Sequence,
        awaiting_continuation: Option<String>,
    },
}

pub struct RunTurnStream {
    data: RunTurnDataStream,
}

impl RunTurnStream {
    #[must_use]
    pub fn next(&self) -> Option<Result<RunTurnStreamItem, LogicError>> {
        self.data.next().map(|result| {
            result
                .map(|item| match item {
                    RunTurnDataStreamItem::Event {
                        event,
                        committed_sequence,
                    } => RunTurnStreamItem::Event {
                        event: map_turn_event(event),
                        committed_sequence,
                    },
                    RunTurnDataStreamItem::Complete {
                        first_committed_sequence,
                        last_committed_sequence,
                        awaiting_continuation,
                    } => RunTurnStreamItem::Complete {
                        first_committed_sequence,
                        last_committed_sequence,
                        awaiting_continuation,
                    },
                })
                .map_err(|_| LogicError::TurnData)
        })
    }
}

/// Logic-owned durable approval command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveApprovalCommand {
    pub session_id: SessionId,
    pub continuation_id: String,
    pub approved: bool,
}

/// Logic-owned durable approval result.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolveApprovalResult {
    pub transitioned: bool,
    pub events: Vec<TurnEvent>,
    pub last_committed_sequence: Option<Sequence>,
    pub awaiting_continuation: Option<String>,
}

/// Narrow logic interface consumed by CLI service.
pub trait CliLogicPort {
    /// Runs the doctor use case.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] when the service command is invalid or required
    /// doctor data is unavailable.
    fn run_doctor(&self, command: RunDoctorCommand) -> Result<DoctorResult, LogicError>;

    /// Creates a durable session.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid input or unavailable runtime data.
    fn create_session(
        &self,
        command: CreateSessionCommand,
    ) -> Result<CreateSessionResult, LogicError>;

    /// Lists bounded session metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for an invalid limit or unavailable runtime data.
    fn list_sessions(
        &self,
        command: ListSessionsCommand,
    ) -> Result<Vec<SessionSummaryResult>, LogicError>;

    /// Purely reconstructs a session.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for unavailable or invalid runtime data.
    fn inspect_session(
        &self,
        command: InspectSessionCommand,
    ) -> Result<InspectSessionResult, LogicError>;

    /// Validates and reads one reconnect page.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid bounds or unavailable session data.
    fn subscribe_session(
        &self,
        command: SubscribeSessionCommand,
    ) -> Result<SessionEventPageResult, LogicError>;

    /// Creates an independently appendable child session.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid style or unavailable runtime data.
    fn branch_session(
        &self,
        command: BranchSessionCommand,
    ) -> Result<BranchSessionResult, LogicError>;

    /// Runs one durable agent turn.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid commands or unavailable runtime data.
    fn run_turn(&self, command: RunTurnCommand) -> Result<RunTurnResult, LogicError>;

    /// Starts a validated incremental turn stream.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid commands or unavailable runtime data.
    fn run_turn_stream(&self, command: RunTurnCommand) -> Result<RunTurnStream, LogicError>;

    /// Cancels one active runtime turn.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid input or unavailable runtime data.
    fn cancel_turn(&self, command: CancelTurnCommand) -> Result<(), LogicError>;

    /// Resolves and resumes a durable approval.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for an invalid continuation or unavailable runtime data.
    fn resolve_approval(
        &self,
        command: ResolveApprovalCommand,
    ) -> Result<ResolveApprovalResult, LogicError>;

    fn upsert_schedule(
        &self,
        _schedule: ScheduleCommand,
    ) -> Result<ScheduleStoreResult, LogicError> {
        Err(LogicError::ScheduleData)
    }

    fn remove_schedule(&self, _schedule_id: &str) -> Result<bool, LogicError> {
        Err(LogicError::ScheduleData)
    }

    fn list_schedules(&self, _limit: u32) -> Result<Vec<ScheduleResult>, LogicError> {
        Err(LogicError::ScheduleData)
    }

    fn claim_due_schedules(
        &self,
        _limit: u32,
    ) -> Result<Vec<ScheduledExecutionResult>, LogicError> {
        Err(LogicError::ScheduleData)
    }

    fn complete_scheduled_execution(
        &self,
        _execution_id: &str,
        _succeeded: bool,
    ) -> Result<bool, LogicError> {
        Err(LogicError::ScheduleData)
    }

    fn run_due_schedules(&self, _limit: u32) -> Result<Vec<ScheduledRunResult>, LogicError> {
        Err(LogicError::ScheduleData)
    }
}

/// CLI logic implementation over an injected data interface.
#[derive(Clone, Debug)]
pub struct CliLogic<D> {
    data: D,
}

impl<D> CliLogic<D> {
    /// Creates CLI logic using only CLI data.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D> CliLogicPort for CliLogic<D>
where
    D: CliDataPort,
{
    fn run_doctor(&self, command: RunDoctorCommand) -> Result<DoctorResult, LogicError> {
        if command.runtime_endpoint.trim().is_empty() {
            return Err(LogicError::InvalidRuntimeEndpoint);
        }
        let record = self
            .data
            .runtime_health(RuntimeHealthDataRequest {
                endpoint_label: command.runtime_endpoint,
            })
            .map_err(|error| LogicError::DoctorData {
                detail: error.to_string(),
            })?;
        let state = match record.availability {
            RuntimeHealthDataAvailability::Ready => DoctorState::Ready,
            RuntimeHealthDataAvailability::Degraded => DoctorState::Degraded,
            RuntimeHealthDataAvailability::Unavailable => DoctorState::Unavailable,
        };
        let successful =
            state == DoctorState::Ready || (!command.strict && state == DoctorState::Degraded);
        Ok(DoctorResult {
            state,
            successful,
            checks: vec![DoctorCheck {
                name: "runtime".into(),
                state,
                detail: format!("runtime version {}", record.version),
            }],
        })
    }

    fn create_session(
        &self,
        command: CreateSessionCommand,
    ) -> Result<CreateSessionResult, LogicError> {
        if command.workspace.trim().is_empty() || command.style.trim().is_empty() {
            return Err(LogicError::InvalidSessionRequest);
        }
        self.data
            .create_session(CreateSessionDataRequest {
                workspace: command.workspace,
                style: command.style,
            })
            .map(|record| CreateSessionResult {
                session_id: record.session_id,
            })
            .map_err(|_| LogicError::SessionData)
    }

    fn list_sessions(
        &self,
        command: ListSessionsCommand,
    ) -> Result<Vec<SessionSummaryResult>, LogicError> {
        if command.limit == 0 || command.limit > 1_000 {
            return Err(LogicError::InvalidSessionLimit);
        }
        self.data
            .list_sessions(ListSessionsDataRequest {
                limit: command.limit,
            })
            .map_err(|_| LogicError::SessionData)
            .map(|sessions| {
                sessions
                    .into_iter()
                    .map(|session| SessionSummaryResult {
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
        command: InspectSessionCommand,
    ) -> Result<InspectSessionResult, LogicError> {
        self.data
            .inspect_session(InspectSessionDataRequest {
                session_id: command.session_id,
                at: command.at,
                replay: command.replay,
            })
            .map(|record| InspectSessionResult {
                session_id: record.session_id,
                head_sequence: record.head_sequence,
                inspected_sequence: record.inspected_sequence,
                event_count: record.event_count,
                state: record.state,
            })
            .map_err(|_| LogicError::SessionHistoryData)
    }

    fn subscribe_session(
        &self,
        command: SubscribeSessionCommand,
    ) -> Result<SessionEventPageResult, LogicError> {
        if command.limit == 0 || command.limit > 1_024 {
            return Err(LogicError::InvalidSessionHistoryRequest);
        }
        self.data
            .subscribe_session(SubscribeSessionDataRequest {
                session_id: command.session_id,
                after: command.after,
                limit: command.limit,
            })
            .map(|record| SessionEventPageResult {
                events: record
                    .events
                    .into_iter()
                    .map(|event| SessionEventResult {
                        sequence: event.sequence,
                        event_type: event.event_type,
                        payload: event.payload,
                    })
                    .collect(),
                head_sequence: record.head_sequence,
                last_delivered_sequence: record.last_delivered_sequence,
                has_more: record.has_more,
            })
            .map_err(|_| LogicError::SessionHistoryData)
    }

    fn branch_session(
        &self,
        command: BranchSessionCommand,
    ) -> Result<BranchSessionResult, LogicError> {
        if command
            .style
            .as_ref()
            .is_some_and(|style| style.trim().is_empty())
        {
            return Err(LogicError::InvalidSessionRequest);
        }
        self.data
            .branch_session(BranchSessionDataRequest {
                session_id: command.session_id,
                at: command.at,
                style: command.style,
            })
            .map(|record| BranchSessionResult {
                session_id: record.session_id,
                parent_session_id: record.parent_session_id,
                fork_sequence: record.fork_sequence,
                child_head_sequence: record.child_head_sequence,
            })
            .map_err(|_| LogicError::SessionHistoryData)
    }

    fn run_turn(&self, command: RunTurnCommand) -> Result<RunTurnResult, LogicError> {
        if command.prompt.trim().is_empty()
            || command.provider.trim().is_empty()
            || command.model.trim().is_empty()
            || !command.options.is_object()
        {
            return Err(LogicError::InvalidTurnRequest);
        }
        self.data
            .run_turn(RunTurnDataRequest {
                session_id: command.session_id,
                prompt: command.prompt,
                provider: command.provider,
                model: command.model,
                options: command.options,
                cancellation_id: command.cancellation_id,
            })
            .map(|record| RunTurnResult {
                events: record.events.into_iter().map(map_turn_event).collect(),
                first_committed_sequence: record.first_committed_sequence,
                last_committed_sequence: record.last_committed_sequence,
                awaiting_continuation: record.awaiting_continuation,
            })
            .map_err(|_| LogicError::TurnData)
    }

    fn run_turn_stream(&self, command: RunTurnCommand) -> Result<RunTurnStream, LogicError> {
        if command.prompt.trim().is_empty()
            || command.provider.trim().is_empty()
            || command.model.trim().is_empty()
            || !command.options.is_object()
        {
            return Err(LogicError::InvalidTurnRequest);
        }
        self.data
            .run_turn_stream(RunTurnDataRequest {
                session_id: command.session_id,
                prompt: command.prompt,
                provider: command.provider,
                model: command.model,
                options: command.options,
                cancellation_id: command.cancellation_id,
            })
            .map(|data| RunTurnStream { data })
            .map_err(|_| LogicError::TurnData)
    }

    fn cancel_turn(&self, command: CancelTurnCommand) -> Result<(), LogicError> {
        if command.reason.trim().is_empty() {
            return Err(LogicError::InvalidCancellationRequest);
        }
        self.data
            .cancel_turn(CancelTurnDataRequest {
                cancellation_id: command.cancellation_id,
                reason: command.reason,
            })
            .map_err(|_| LogicError::CancellationData)
    }

    fn resolve_approval(
        &self,
        command: ResolveApprovalCommand,
    ) -> Result<ResolveApprovalResult, LogicError> {
        if command.continuation_id.trim().is_empty() {
            return Err(LogicError::InvalidApprovalRequest);
        }
        self.data
            .resolve_approval(ResolveApprovalDataRequest {
                session_id: command.session_id,
                continuation_id: command.continuation_id,
                approved: command.approved,
            })
            .map(|record| ResolveApprovalResult {
                transitioned: record.transitioned,
                events: record.events.into_iter().map(map_turn_event).collect(),
                last_committed_sequence: record.last_committed_sequence,
                awaiting_continuation: record.awaiting_continuation,
            })
            .map_err(|_| LogicError::ApprovalData)
    }

    fn upsert_schedule(
        &self,
        schedule: ScheduleCommand,
    ) -> Result<ScheduleStoreResult, LogicError> {
        validate_schedule(&schedule)?;
        self.data
            .upsert_schedule(to_data_schedule(schedule))
            .map(|value| ScheduleStoreResult {
                schedule_id: value.schedule_id,
                replayed: value.replayed,
            })
            .map_err(|_| LogicError::ScheduleData)
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, LogicError> {
        validate_schedule_id(schedule_id)?;
        self.data
            .remove_schedule(schedule_id)
            .map_err(|_| LogicError::ScheduleData)
    }

    fn list_schedules(&self, limit: u32) -> Result<Vec<ScheduleResult>, LogicError> {
        validate_schedule_limit(limit)?;
        self.data
            .list_schedules(limit)
            .map(|values| values.into_iter().map(from_data_schedule).collect())
            .map_err(|_| LogicError::ScheduleData)
    }

    fn claim_due_schedules(&self, limit: u32) -> Result<Vec<ScheduledExecutionResult>, LogicError> {
        validate_schedule_limit(limit)?;
        self.data
            .claim_due_schedules(limit)
            .map(|values| values.into_iter().map(from_data_execution).collect())
            .map_err(|_| LogicError::ScheduleData)
    }

    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, LogicError> {
        if execution_id.len() != 64 || !execution_id.bytes().all(|value| value.is_ascii_hexdigit())
        {
            return Err(LogicError::InvalidScheduleRequest);
        }
        self.data
            .complete_scheduled_execution(execution_id, succeeded)
            .map_err(|_| LogicError::ScheduleData)
    }

    fn run_due_schedules(&self, limit: u32) -> Result<Vec<ScheduledRunResult>, LogicError> {
        validate_schedule_limit(limit)?;
        self.data
            .run_due_schedules(limit)
            .map(|values| values.into_iter().map(from_data_run).collect())
            .map_err(|_| LogicError::ScheduleData)
    }
}

fn validate_schedule(value: &ScheduleCommand) -> Result<(), LogicError> {
    validate_schedule_id(&value.schedule_id)?;
    validate_schedule_id(&value.idempotency_id)?;
    if value.style.trim().is_empty()
        || value.workspace.trim().is_empty()
        || value.permission_policy.trim().is_empty()
        || value.provider.trim().is_empty()
        || value.model.trim().is_empty()
    {
        return Err(LogicError::InvalidScheduleRequest);
    }
    match &value.trigger {
        ScheduleTrigger::AtMillis(value) if *value >= 0 => {}
        ScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        } if *starts_at_ms >= 0 && *every_ms >= 1_000 => {}
        ScheduleTrigger::RuntimeEvent { event_type } if !event_type.trim().is_empty() => {}
        ScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } if !process_id.trim().is_empty() && !contains.is_empty() => {}
        _ => return Err(LogicError::InvalidScheduleRequest),
    }
    match &value.payload {
        SchedulePayload::Prompt { prompt } if !prompt.trim().is_empty() => {}
        SchedulePayload::Continuation { continuation_id } if !continuation_id.trim().is_empty() => {
        }
        _ => return Err(LogicError::InvalidScheduleRequest),
    }
    Ok(())
}

fn validate_schedule_id(value: &str) -> Result<(), LogicError> {
    if !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        Ok(())
    } else {
        Err(LogicError::InvalidScheduleRequest)
    }
}

fn validate_schedule_limit(limit: u32) -> Result<(), LogicError> {
    if (1..=1_000).contains(&limit) {
        Ok(())
    } else {
        Err(LogicError::InvalidScheduleRequest)
    }
}

fn to_data_schedule(value: ScheduleCommand) -> ScheduleDataRecord {
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
            ScheduleTrigger::AtMillis(value) => ScheduleDataTrigger::AtMillis(value),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleDataTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleTrigger::RuntimeEvent { event_type } => {
                ScheduleDataTrigger::RuntimeEvent { event_type }
            }
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleDataTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            SchedulePayload::Prompt { prompt } => ScheduleDataPayload::Prompt { prompt },
            SchedulePayload::Continuation { continuation_id } => {
                ScheduleDataPayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_data_schedule(value: ScheduleDataRecord) -> ScheduleResult {
    ScheduleResult {
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
            ScheduleDataTrigger::AtMillis(value) => ScheduleTrigger::AtMillis(value),
            ScheduleDataTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleDataTrigger::RuntimeEvent { event_type } => {
                ScheduleTrigger::RuntimeEvent { event_type }
            }
            ScheduleDataTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            ScheduleDataPayload::Prompt { prompt } => SchedulePayload::Prompt { prompt },
            ScheduleDataPayload::Continuation { continuation_id } => {
                SchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_data_execution(value: ScheduledExecutionDataRecord) -> ScheduledExecutionResult {
    ScheduledExecutionResult {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        schedule: from_data_schedule(value.schedule),
    }
}

fn from_data_run(value: ScheduledRunDataRecord) -> ScheduledRunResult {
    ScheduledRunResult {
        execution_id: value.execution_id,
        schedule_id: value.schedule_id,
        terminal: value.terminal,
        succeeded: value.succeeded,
        last_committed_sequence: value.last_committed_sequence,
        awaiting_continuation: value.awaiting_continuation,
        error: value.error,
    }
}

fn map_turn_event(event: TurnDataEvent) -> TurnEvent {
    match event {
        TurnDataEvent::Started => TurnEvent::Started,
        TurnDataEvent::Text(value) => TurnEvent::Text(value),
        TurnDataEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => TurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        TurnDataEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => TurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        TurnDataEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        } => TurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        },
        TurnDataEvent::Cancelled => TurnEvent::Cancelled,
        TurnDataEvent::Failed {
            code,
            message,
            retryable,
        } => TurnEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

/// CLI business failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LogicError {
    /// The service supplied no selected runtime endpoint.
    #[error("runtime endpoint is empty")]
    InvalidRuntimeEndpoint,
    /// Doctor data could not be constructed.
    #[error("doctor data unavailable: {detail}")]
    DoctorData {
        /// Sanitized data-layer diagnostic.
        detail: String,
    },
    /// Session request is incomplete.
    #[error("session request is invalid")]
    InvalidSessionRequest,
    /// Session list bound is invalid.
    #[error("session list limit is invalid")]
    InvalidSessionLimit,
    /// Session runtime data is unavailable.
    #[error("session runtime data is unavailable")]
    SessionData,
    #[error("session history data is unavailable")]
    SessionHistoryData,
    #[error("session history request is invalid")]
    InvalidSessionHistoryRequest,
    /// Turn request is invalid.
    #[error("turn request is invalid")]
    InvalidTurnRequest,
    /// Turn runtime data is unavailable.
    #[error("turn runtime data is unavailable")]
    TurnData,
    /// Cancellation request is incomplete.
    #[error("cancellation request is invalid")]
    InvalidCancellationRequest,
    /// Cancellation runtime data is unavailable.
    #[error("cancellation runtime data is unavailable")]
    CancellationData,
    /// Approval request is incomplete.
    #[error("approval request is invalid")]
    InvalidApprovalRequest,
    /// Approval runtime data is unavailable.
    #[error("approval runtime data is unavailable")]
    ApprovalData,
    #[error("schedule request is invalid")]
    InvalidScheduleRequest,
    #[error("schedule runtime data is unavailable")]
    ScheduleData,
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_cli_data::{DataError, RuntimeHealthDataRecord};

    use super::*;

    struct MockData {
        availability: RuntimeHealthDataAvailability,
        observed: RefCell<Vec<RuntimeHealthDataRequest>>,
    }

    impl CliDataPort for MockData {
        fn runtime_health(
            &self,
            request: RuntimeHealthDataRequest,
        ) -> Result<RuntimeHealthDataRecord, DataError> {
            self.observed.borrow_mut().push(request);
            Ok(RuntimeHealthDataRecord {
                availability: self.availability,
                version: "1.2.3".into(),
            })
        }

        fn create_session(
            &self,
            _request: CreateSessionDataRequest,
        ) -> Result<agentmod_cli_data::CreateSessionDataRecord, DataError> {
            Ok(agentmod_cli_data::CreateSessionDataRecord {
                session_id: SessionId::from_uuid(uuid::Uuid::from_u128(1)),
            })
        }

        fn list_sessions(
            &self,
            _request: ListSessionsDataRequest,
        ) -> Result<Vec<agentmod_cli_data::SessionSummaryDataRecord>, DataError> {
            Ok(vec![])
        }

        fn inspect_session(
            &self,
            request: InspectSessionDataRequest,
        ) -> Result<agentmod_cli_data::InspectSessionDataRecord, DataError> {
            Ok(agentmod_cli_data::InspectSessionDataRecord {
                session_id: request.session_id,
                head_sequence: Sequence::FIRST,
                inspected_sequence: Sequence::FIRST,
                event_count: 1,
                state: serde_json::json!({"fixture": true}),
            })
        }

        fn subscribe_session(
            &self,
            request: SubscribeSessionDataRequest,
        ) -> Result<agentmod_cli_data::SessionEventPageDataRecord, DataError> {
            Ok(agentmod_cli_data::SessionEventPageDataRecord {
                events: vec![],
                head_sequence: Sequence::FIRST,
                last_delivered_sequence: request.after,
                has_more: false,
            })
        }

        fn branch_session(
            &self,
            request: BranchSessionDataRequest,
        ) -> Result<agentmod_cli_data::BranchSessionDataRecord, DataError> {
            Ok(agentmod_cli_data::BranchSessionDataRecord {
                session_id: SessionId::from_uuid(uuid::Uuid::from_u128(2)),
                parent_session_id: request.session_id,
                fork_sequence: request.at,
                child_head_sequence: Sequence::new(2).expect("sequence"),
            })
        }

        fn run_turn(
            &self,
            _request: RunTurnDataRequest,
        ) -> Result<agentmod_cli_data::RunTurnDataRecord, DataError> {
            Ok(agentmod_cli_data::RunTurnDataRecord {
                events: vec![],
                first_committed_sequence: Sequence::FIRST,
                last_committed_sequence: Sequence::FIRST,
                awaiting_continuation: None,
            })
        }

        fn run_turn_stream(
            &self,
            _request: RunTurnDataRequest,
        ) -> Result<RunTurnDataStream, DataError> {
            Err(DataError::InvalidEndpointLabel)
        }

        fn cancel_turn(&self, _request: CancelTurnDataRequest) -> Result<(), DataError> {
            Ok(())
        }

        fn resolve_approval(
            &self,
            _request: ResolveApprovalDataRequest,
        ) -> Result<agentmod_cli_data::ResolveApprovalDataRecord, DataError> {
            Ok(agentmod_cli_data::ResolveApprovalDataRecord {
                transitioned: true,
                events: vec![],
                last_committed_sequence: Some(Sequence::FIRST),
                awaiting_continuation: None,
            })
        }
    }

    fn logic(availability: RuntimeHealthDataAvailability) -> CliLogic<MockData> {
        CliLogic::new(MockData {
            availability,
            observed: RefCell::new(Vec::new()),
        })
    }

    #[test]
    fn ready_runtime_produces_successful_doctor_result() {
        let logic = logic(RuntimeHealthDataAvailability::Ready);
        let result = logic
            .run_doctor(RunDoctorCommand {
                strict: true,
                runtime_endpoint: "local".into(),
            })
            .expect("doctor result");
        assert_eq!(result.state, DoctorState::Ready);
        assert!(result.successful);
        assert_eq!(result.checks[0].name, "runtime");
        assert_eq!(
            logic.data.observed.into_inner(),
            vec![RuntimeHealthDataRequest {
                endpoint_label: "local".into(),
            }]
        );
    }

    #[test]
    fn degraded_runtime_is_allowed_only_outside_strict_mode() {
        let normal = logic(RuntimeHealthDataAvailability::Degraded)
            .run_doctor(RunDoctorCommand {
                strict: false,
                runtime_endpoint: "local".into(),
            })
            .expect("normal doctor");
        assert!(normal.successful);

        let strict = logic(RuntimeHealthDataAvailability::Degraded)
            .run_doctor(RunDoctorCommand {
                strict: true,
                runtime_endpoint: "local".into(),
            })
            .expect("strict doctor");
        assert!(!strict.successful);
    }

    #[test]
    fn unavailable_runtime_always_fails_doctor_policy() {
        let result = logic(RuntimeHealthDataAvailability::Unavailable)
            .run_doctor(RunDoctorCommand {
                strict: false,
                runtime_endpoint: "local".into(),
            })
            .expect("doctor result");
        assert_eq!(result.state, DoctorState::Unavailable);
        assert!(!result.successful);
    }
}
