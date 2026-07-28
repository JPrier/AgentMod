//! Runtime endpoint mapping and lifecycle.

pub mod continuation;
pub mod harness;
pub mod local_rpc;
pub mod turn;

use std::path::PathBuf;

use agentmod_runtime_logic::{
    GetRuntimeHealthCommand, LogicError, RuntimeHealthState, RuntimeLogicPort,
    history::{
        BranchSessionCommand, InspectSessionCommand, SessionHistoryLogicPort,
        SubscribeSessionCommand,
    },
    registry::{
        CreateSessionCommand, ListSessionsCommand, SessionRegistryLogicError,
        SessionRegistryLogicPort,
    },
    scheduler::{
        FireProcessOutputCommand, FireRuntimeEventCommand, RuntimeSchedule,
        RuntimeScheduleLogicError, RuntimeScheduleLogicPort, SchedulePayload, ScheduleTrigger,
        ScheduledExecution, UpsertScheduleCommand,
    },
};
use agentmod_runtime_protocol::{
    RuntimeRequest, RuntimeResponse, RuntimeSchedulePayload, RuntimeScheduleSpec,
    RuntimeScheduleTrigger, RuntimeScheduledExecution, SessionSummary,
};
use thiserror::Error;

/// Service-owned health request after transport parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHealthRequest {
    /// Storage configuration copied from the service bootstrap context.
    pub configured_session_root: PathBuf,
}

/// Service-owned health response before wire mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHealthResponse {
    /// Endpoint-safe status.
    pub status: ServiceHealthStatus,
    /// Application version included in endpoint output.
    pub version: String,
}

/// Endpoint-safe health status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceHealthStatus {
    /// Runtime is ready.
    Ok,
    /// Runtime can respond but a required capability is unavailable.
    Degraded,
}

/// Runtime service configuration, not a logic business command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeServiceConfig {
    /// Canonical sessions root selected at bootstrap.
    pub session_root: PathBuf,
    /// Build version.
    pub version: String,
}

/// Endpoint-facing runtime service.
#[derive(Clone, Debug)]
pub struct RuntimeService<L> {
    logic: L,
    config: RuntimeServiceConfig,
}

impl<L> RuntimeService<L> {
    /// Creates a service with injected logic and endpoint bootstrap settings.
    #[must_use]
    pub const fn new(logic: L, config: RuntimeServiceConfig) -> Self {
        Self { logic, config }
    }
}

impl<L> RuntimeService<L>
where
    L: RuntimeLogicPort + SessionRegistryLogicPort + SessionHistoryLogicPort,
{
    /// Handles the currently implemented runtime wire endpoints.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for unsupported endpoints, invalid service
    /// configuration, or translated business failures.
    pub fn handle_wire(&self, request: &RuntimeRequest) -> Result<RuntimeResponse, ServiceError> {
        match request {
            RuntimeRequest::Health => {
                let service_request = ServiceHealthRequest {
                    configured_session_root: self.config.session_root.clone(),
                };
                let service_response = self.health(service_request)?;
                Ok(RuntimeResponse::Health {
                    status: match service_response.status {
                        ServiceHealthStatus::Ok => "ok",
                        ServiceHealthStatus::Degraded => "degraded",
                    }
                    .into(),
                    version: service_response.version,
                })
            }
            RuntimeRequest::CreateSession { workspace, style } => {
                let service_request = ServiceCreateSessionRequest {
                    workspace: workspace.clone(),
                    style: style.clone(),
                };
                let created = self.create_session(service_request)?;
                Ok(RuntimeResponse::SessionCreated {
                    session_id: created.session_id,
                })
            }
            RuntimeRequest::ListSessions { limit } => {
                let listed = self.list_sessions(ServiceListSessionsRequest { limit: *limit })?;
                Ok(RuntimeResponse::Sessions {
                    sessions: listed
                        .sessions
                        .into_iter()
                        .map(|session| SessionSummary {
                            id: session.id,
                            workspace_label: session.workspace_label,
                            style: session.style,
                            sequence: session.sequence,
                            state: session.state,
                        })
                        .collect(),
                })
            }
            RuntimeRequest::InspectSession { session_id, at }
            | RuntimeRequest::ReplaySession { session_id, at } => {
                let inspected = self.inspect_session(ServiceInspectSessionRequest {
                    session_id: *session_id,
                    at: *at,
                })?;
                Ok(RuntimeResponse::SessionInspected {
                    session_id: inspected.session_id,
                    head_sequence: inspected.head_sequence,
                    inspected_sequence: inspected.inspected_sequence,
                    event_count: inspected.event_count,
                    state: inspected.state,
                })
            }
            RuntimeRequest::BranchSession {
                session_id,
                at,
                style,
            } => {
                let branched = self.branch_session(ServiceBranchSessionRequest {
                    parent_session_id: *session_id,
                    at: *at,
                    style: style.clone(),
                })?;
                Ok(RuntimeResponse::SessionBranched {
                    session_id: branched.session_id,
                    parent_session_id: branched.parent_session_id,
                    fork_sequence: branched.fork_sequence,
                    child_head_sequence: branched.child_head_sequence,
                })
            }
            _ => Err(ServiceError::UnsupportedEndpoint),
        }
    }

    /// Purely reconstructs endpoint-safe structured state.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when history replay or endpoint serialization fails.
    pub fn inspect_session(
        &self,
        request: ServiceInspectSessionRequest,
    ) -> Result<ServiceInspectSessionResponse, ServiceError> {
        let result = self
            .logic
            .inspect_session(InspectSessionCommand {
                sessions_root: self.config.session_root.clone(),
                session_id: request.session_id,
                at: request.at,
            })
            .map_err(|error| ServiceError::SessionHistory(error.to_string()))?;
        let state =
            serde_json::to_value(&result.state).map_err(|_| ServiceError::StateSerialization)?;
        Ok(ServiceInspectSessionResponse {
            session_id: result.state.id,
            head_sequence: result.head_sequence,
            inspected_sequence: result.inspected_sequence,
            event_count: result.event_count,
            state,
        })
    }

    /// Reads one bounded verified event page for a reconnecting frontend.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when the cursor, bound, session, or journal is invalid.
    pub fn subscribe_session(
        &self,
        request: ServiceSubscribeSessionRequest,
    ) -> Result<ServiceSessionEventPage, ServiceError> {
        let result = self
            .logic
            .subscribe_session(SubscribeSessionCommand {
                sessions_root: self.config.session_root.clone(),
                session_id: request.session_id,
                after: request.after,
                limit: request.limit,
            })
            .map_err(|error| ServiceError::SessionHistory(error.to_string()))?;
        Ok(ServiceSessionEventPage {
            head_sequence: result.head_sequence,
            last_delivered_sequence: result.last_delivered_sequence,
            has_more: result.has_more,
            events: result
                .events
                .into_iter()
                .map(|event| ServiceSessionEvent {
                    event_id: event.event_id,
                    sequence: event.sequence,
                    event_type: event.event_type,
                    payload: event.payload,
                })
                .collect(),
        })
    }

    /// Creates an atomic replay-derived branch.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when validation, replay, or atomic persistence fails.
    pub fn branch_session(
        &self,
        request: ServiceBranchSessionRequest,
    ) -> Result<ServiceBranchSessionResponse, ServiceError> {
        let result = self
            .logic
            .branch_session(BranchSessionCommand {
                sessions_root: self.config.session_root.clone(),
                parent_session_id: request.parent_session_id,
                at: request.at,
                style: request.style,
            })
            .map_err(|error| ServiceError::SessionHistory(error.to_string()))?;
        Ok(ServiceBranchSessionResponse {
            session_id: result.session_id,
            parent_session_id: result.parent_session_id,
            fork_sequence: result.fork_sequence,
            child_head_sequence: result.child_head_sequence,
        })
    }

    /// Creates a session through service-owned request and result types.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for endpoint validation or translated business failures.
    pub fn create_session(
        &self,
        request: ServiceCreateSessionRequest,
    ) -> Result<ServiceCreateSessionResponse, ServiceError> {
        if request.workspace.trim().is_empty() || request.style.trim().is_empty() {
            return Err(ServiceError::InvalidSessionRequest);
        }
        let result = self
            .logic
            .create_session(CreateSessionCommand {
                sessions_root: self.config.session_root.clone(),
                workspace: PathBuf::from(request.workspace),
                style: request.style,
            })
            .map_err(ServiceError::SessionRegistry)?;
        Ok(ServiceCreateSessionResponse {
            session_id: result.session_id,
        })
    }

    /// Lists lightweight dormant-session metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for an invalid bound or translated business failure.
    pub fn list_sessions(
        &self,
        request: ServiceListSessionsRequest,
    ) -> Result<ServiceListSessionsResponse, ServiceError> {
        let limit =
            usize::try_from(request.limit).map_err(|_| ServiceError::InvalidSessionListLimit)?;
        let sessions = self
            .logic
            .list_sessions(ListSessionsCommand {
                sessions_root: self.config.session_root.clone(),
                limit,
            })
            .map_err(ServiceError::SessionRegistry)?
            .into_iter()
            .map(|record| ServiceSessionSummary {
                id: record.id,
                workspace_label: record.workspace_label,
                style: record.style,
                sequence: record.sequence,
                state: record.state,
            })
            .collect();
        Ok(ServiceListSessionsResponse { sessions })
    }

    /// Executes the service-owned health endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::InvalidSessionRoot`] for invalid service input or
    /// [`ServiceError::Logic`] for a translated business failure.
    pub fn health(
        &self,
        request: ServiceHealthRequest,
    ) -> Result<ServiceHealthResponse, ServiceError> {
        if request.configured_session_root.as_os_str().is_empty() {
            return Err(ServiceError::InvalidSessionRoot);
        }
        let command = GetRuntimeHealthCommand {
            canonical_session_root: request.configured_session_root,
        };
        let result = self
            .logic
            .get_health(command)
            .map_err(ServiceError::Logic)?;
        Ok(ServiceHealthResponse {
            status: match result.state {
                RuntimeHealthState::Ready => ServiceHealthStatus::Ok,
                RuntimeHealthState::Degraded => ServiceHealthStatus::Degraded,
            },
            version: self.config.version.clone(),
        })
    }
}

impl<L: RuntimeScheduleLogicPort> RuntimeService<L> {
    /// Maps runtime schedule endpoints through service-owned and logic-owned types.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for invalid business requests or scheduler failures.
    pub fn handle_schedule_wire(
        &self,
        request: &RuntimeRequest,
    ) -> Result<RuntimeResponse, ServiceError> {
        match request {
            RuntimeRequest::UpsertSchedule { schedule } => {
                let result = self.upsert_schedule(from_wire_schedule((**schedule).clone()))?;
                Ok(RuntimeResponse::ScheduleStored {
                    schedule_id: result.schedule_id,
                    replayed: result.replayed,
                })
            }
            RuntimeRequest::RemoveSchedule { schedule_id } => {
                let existed = self
                    .logic
                    .remove_schedule(schedule_id)
                    .map_err(ServiceError::Schedule)?;
                Ok(RuntimeResponse::ScheduleRemoved { existed })
            }
            RuntimeRequest::ListSchedules { limit } => {
                let schedules = self
                    .logic
                    .list_schedules(*limit)
                    .map_err(ServiceError::Schedule)?
                    .into_iter()
                    .map(from_logic_schedule)
                    .map(to_wire_schedule)
                    .collect();
                Ok(RuntimeResponse::Schedules { schedules })
            }
            RuntimeRequest::ClaimDueSchedules { limit } => {
                let executions = self
                    .logic
                    .claim_due_schedules(*limit)
                    .map_err(ServiceError::Schedule)?
                    .into_iter()
                    .map(|execution| RuntimeScheduledExecution {
                        execution_id: execution.execution_id,
                        scheduled_for_ms: execution.scheduled_for_ms,
                        claimed_at_ms: execution.claimed_at_ms,
                        schedule: to_wire_schedule(from_logic_schedule(execution.schedule)),
                    })
                    .collect();
                Ok(RuntimeResponse::ScheduledExecutions { executions })
            }
            RuntimeRequest::CompleteScheduledExecution {
                execution_id,
                succeeded,
            } => {
                let changed = self
                    .logic
                    .complete_scheduled_execution(execution_id, *succeeded)
                    .map_err(ServiceError::Schedule)?;
                Ok(RuntimeResponse::ScheduledExecutionCompleted { changed })
            }
            _ => Err(ServiceError::UnsupportedEndpoint),
        }
    }

    fn upsert_schedule(
        &self,
        schedule: ServiceSchedule,
    ) -> Result<ServiceScheduleStoreResult, ServiceError> {
        let result = self
            .logic
            .upsert_schedule(UpsertScheduleCommand {
                schedule_id: schedule.schedule_id,
                session_id: schedule.session_id,
                idempotency_id: schedule.idempotency_id,
                style: schedule.style,
                workspace: schedule.workspace,
                permission_policy: schedule.permission_policy,
                provider: schedule.provider,
                model: schedule.model,
                token_budget: schedule.token_budget,
                cost_budget_micros: schedule.cost_budget_micros,
                trigger: to_logic_trigger(schedule.trigger),
                payload: to_logic_payload(schedule.payload),
                active: schedule.active,
            })
            .map_err(ServiceError::Schedule)?;
        Ok(ServiceScheduleStoreResult {
            schedule_id: result.schedule_id,
            replayed: result.replayed,
        })
    }

    fn fire_runtime_event(
        &self,
        event_id: String,
        event_type: String,
    ) -> Result<Vec<ServiceScheduledExecution>, ServiceError> {
        self.logic
            .fire_runtime_event(FireRuntimeEventCommand {
                event_id,
                event_type,
            })
            .map(|values| values.into_iter().map(from_logic_execution).collect())
            .map_err(ServiceError::Schedule)
    }

    fn fire_process_output(
        &self,
        output_id: String,
        process_id: String,
        output: String,
    ) -> Result<Vec<ServiceScheduledExecution>, ServiceError> {
        self.logic
            .fire_process_output(FireProcessOutputCommand {
                output_id,
                process_id,
                output,
            })
            .map(|values| values.into_iter().map(from_logic_execution).collect())
            .map_err(ServiceError::Schedule)
    }
}

/// Service-owned create-session request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCreateSessionRequest {
    /// Endpoint workspace text.
    pub workspace: String,
    /// Endpoint style text.
    pub style: String,
}

/// Service-owned create-session response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCreateSessionResponse {
    /// Canonical identifier.
    pub session_id: agentmod_primitives::SessionId,
}

/// Service-owned list request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceListSessionsRequest {
    /// Caller-requested bound.
    pub limit: u32,
}

/// Service-owned summary record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceSessionSummary {
    /// Session ID.
    pub id: agentmod_primitives::SessionId,
    /// Safe workspace label.
    pub workspace_label: String,
    /// Explicit style.
    pub style: String,
    /// Last known sequence.
    pub sequence: agentmod_primitives::Sequence,
    /// Lifecycle label.
    pub state: String,
}

/// Service-owned list response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceListSessionsResponse {
    /// Bounded summaries.
    pub sessions: Vec<ServiceSessionSummary>,
}

/// Service-owned point-in-time request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceInspectSessionRequest {
    /// Selected session.
    pub session_id: agentmod_primitives::SessionId,
    /// Inclusive target.
    pub at: Option<agentmod_primitives::Sequence>,
}

/// Service-owned point-in-time response.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceInspectSessionResponse {
    /// Selected session.
    pub session_id: agentmod_primitives::SessionId,
    /// Verified head.
    pub head_sequence: agentmod_primitives::Sequence,
    /// Replayed point.
    pub inspected_sequence: agentmod_primitives::Sequence,
    /// Events reduced.
    pub event_count: u64,
    /// Endpoint-safe structured state.
    pub state: serde_json::Value,
}

/// Service-owned reconnect cursor request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceSubscribeSessionRequest {
    /// Selected session.
    pub session_id: agentmod_primitives::SessionId,
    /// Last contiguous event already received.
    pub after: Option<agentmod_primitives::Sequence>,
    /// Maximum page size.
    pub limit: u32,
}

/// One service-owned canonical event projection.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceSessionEvent {
    /// Canonical event identity.
    pub event_id: agentmod_primitives::EventId,
    /// Canonical sequence.
    pub sequence: agentmod_primitives::Sequence,
    /// Stable event type.
    pub event_type: String,
    /// Typed payload.
    pub payload: serde_json::Value,
}

/// Service-owned bounded reconnect page.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceSessionEventPage {
    /// Verified journal head.
    pub head_sequence: agentmod_primitives::Sequence,
    /// Last sequence in the page.
    pub last_delivered_sequence: Option<agentmod_primitives::Sequence>,
    /// Whether an immediate next page exists.
    pub has_more: bool,
    /// Ordered events.
    pub events: Vec<ServiceSessionEvent>,
}

/// Service-owned branch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceBranchSessionRequest {
    /// Immutable parent.
    pub parent_session_id: agentmod_primitives::SessionId,
    /// Inclusive fork point.
    pub at: agentmod_primitives::Sequence,
    /// Optional child style replacement.
    pub style: Option<String>,
}

/// Service-owned branch response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceBranchSessionResponse {
    /// Fresh child.
    pub session_id: agentmod_primitives::SessionId,
    /// Immutable parent.
    pub parent_session_id: agentmod_primitives::SessionId,
    /// Parent fork point.
    pub fork_sequence: agentmod_primitives::Sequence,
    /// Child journal head.
    pub child_head_sequence: agentmod_primitives::Sequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceSchedule {
    schedule_id: String,
    session_id: agentmod_primitives::SessionId,
    idempotency_id: String,
    style: String,
    workspace: String,
    permission_policy: String,
    provider: String,
    model: String,
    token_budget: u64,
    cost_budget_micros: u64,
    trigger: ServiceScheduleTrigger,
    payload: ServiceSchedulePayload,
    active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ServiceScheduleTrigger {
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
enum ServiceSchedulePayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceScheduleStoreResult {
    schedule_id: String,
    replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceScheduledExecution {
    execution_id: String,
    scheduled_for_ms: i64,
    claimed_at_ms: i64,
    schedule: ServiceSchedule,
}

fn from_wire_schedule(value: RuntimeScheduleSpec) -> ServiceSchedule {
    ServiceSchedule {
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
            RuntimeScheduleTrigger::AtMillis(value) => ServiceScheduleTrigger::AtMillis(value),
            RuntimeScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ServiceScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            RuntimeScheduleTrigger::RuntimeEvent { event_type } => {
                ServiceScheduleTrigger::RuntimeEvent { event_type }
            }
            RuntimeScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ServiceScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            RuntimeSchedulePayload::Prompt { prompt } => ServiceSchedulePayload::Prompt { prompt },
            RuntimeSchedulePayload::Continuation { continuation_id } => {
                ServiceSchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn to_logic_trigger(value: ServiceScheduleTrigger) -> ScheduleTrigger {
    match value {
        ServiceScheduleTrigger::AtMillis(value) => ScheduleTrigger::AtMillis(value),
        ServiceScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        } => ScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        },
        ServiceScheduleTrigger::RuntimeEvent { event_type } => {
            ScheduleTrigger::RuntimeEvent { event_type }
        }
        ServiceScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => ScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        },
    }
}

fn to_logic_payload(value: ServiceSchedulePayload) -> SchedulePayload {
    match value {
        ServiceSchedulePayload::Prompt { prompt } => SchedulePayload::Prompt { prompt },
        ServiceSchedulePayload::Continuation { continuation_id } => {
            SchedulePayload::Continuation { continuation_id }
        }
    }
}

fn from_logic_schedule(value: RuntimeSchedule) -> ServiceSchedule {
    ServiceSchedule {
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
            ScheduleTrigger::AtMillis(value) => ServiceScheduleTrigger::AtMillis(value),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ServiceScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleTrigger::RuntimeEvent { event_type } => {
                ServiceScheduleTrigger::RuntimeEvent { event_type }
            }
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ServiceScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            SchedulePayload::Prompt { prompt } => ServiceSchedulePayload::Prompt { prompt },
            SchedulePayload::Continuation { continuation_id } => {
                ServiceSchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_logic_execution(value: ScheduledExecution) -> ServiceScheduledExecution {
    ServiceScheduledExecution {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        claimed_at_ms: value.claimed_at_ms,
        schedule: from_logic_schedule(value.schedule),
    }
}

fn to_wire_schedule(value: ServiceSchedule) -> RuntimeScheduleSpec {
    RuntimeScheduleSpec {
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
            ServiceScheduleTrigger::AtMillis(value) => RuntimeScheduleTrigger::AtMillis(value),
            ServiceScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => RuntimeScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ServiceScheduleTrigger::RuntimeEvent { event_type } => {
                RuntimeScheduleTrigger::RuntimeEvent { event_type }
            }
            ServiceScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => RuntimeScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            ServiceSchedulePayload::Prompt { prompt } => RuntimeSchedulePayload::Prompt { prompt },
            ServiceSchedulePayload::Continuation { continuation_id } => {
                RuntimeSchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

/// Runtime endpoint error.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ServiceError {
    /// Endpoint is part of the wire contract but not implemented in this slice.
    #[error("runtime endpoint is not available")]
    UnsupportedEndpoint,
    /// Service bootstrap/configuration supplied an invalid path.
    #[error("configured session root is empty")]
    InvalidSessionRoot,
    /// Business use case failed.
    #[error("runtime operation failed: {0}")]
    Logic(LogicError),
    /// Create request failed endpoint validation.
    #[error("create-session request is invalid")]
    InvalidSessionRequest,
    /// Platform cannot represent the requested list bound.
    #[error("session list limit is invalid")]
    InvalidSessionListLimit,
    /// Session registry business use case failed.
    #[error("session registry operation failed: {0}")]
    SessionRegistry(SessionRegistryLogicError),
    /// Point-in-time replay or branching failed.
    #[error("session history operation failed: {0}")]
    SessionHistory(String),
    /// Durable scheduler operation failed.
    #[error("scheduler operation failed: {0}")]
    Schedule(RuntimeScheduleLogicError),
    /// Replay state could not be rendered at the endpoint boundary.
    #[error("session state could not be serialized")]
    StateSerialization,
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_logic::RuntimeHealthResult;

    use super::*;

    struct MockLogic {
        state: RuntimeHealthState,
        observed: RefCell<Vec<GetRuntimeHealthCommand>>,
    }

    impl RuntimeLogicPort for MockLogic {
        fn get_health(
            &self,
            command: GetRuntimeHealthCommand,
        ) -> Result<RuntimeHealthResult, LogicError> {
            self.observed.borrow_mut().push(command);
            Ok(RuntimeHealthResult {
                state: self.state,
                diagnostics: vec![],
            })
        }
    }

    impl SessionRegistryLogicPort for MockLogic {
        fn create_session(
            &self,
            _command: CreateSessionCommand,
        ) -> Result<agentmod_runtime_logic::registry::CreateSessionResult, SessionRegistryLogicError>
        {
            Err(SessionRegistryLogicError::InvalidWorkspace)
        }

        fn list_sessions(
            &self,
            _command: ListSessionsCommand,
        ) -> Result<
            Vec<agentmod_runtime_logic::registry::SessionSummaryResult>,
            SessionRegistryLogicError,
        > {
            Ok(vec![])
        }
    }

    impl SessionHistoryLogicPort for MockLogic {
        fn inspect_session(
            &self,
            _command: InspectSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::InspectSessionResult,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }

        fn subscribe_session(
            &self,
            _command: agentmod_runtime_logic::history::SubscribeSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::SessionEventPage,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }

        fn branch_session(
            &self,
            _command: BranchSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::BranchSessionResult,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }
    }

    fn service(state: RuntimeHealthState) -> RuntimeService<MockLogic> {
        RuntimeService::new(
            MockLogic {
                state,
                observed: RefCell::new(Vec::new()),
            },
            RuntimeServiceConfig {
                session_root: PathBuf::from("sessions"),
                version: "0.1.0-test".into(),
            },
        )
    }

    #[test]
    fn wire_health_is_mapped_through_service_and_logic_types() {
        let service = service(RuntimeHealthState::Ready);
        assert_eq!(
            service
                .handle_wire(&RuntimeRequest::Health)
                .expect("health"),
            RuntimeResponse::Health {
                status: "ok".into(),
                version: "0.1.0-test".into(),
            }
        );
        assert_eq!(
            service.logic.observed.into_inner(),
            vec![GetRuntimeHealthCommand {
                canonical_session_root: PathBuf::from("sessions")
            }]
        );
    }

    #[test]
    fn unsupported_wire_request_is_explicit() {
        assert_eq!(
            service(RuntimeHealthState::Ready).handle_wire(&RuntimeRequest::Cancel {
                cancellation_id: agentmod_primitives::CancellationId::from_uuid(
                    uuid::Uuid::from_u128(1),
                ),
                reason: String::from("fixture"),
            }),
            Err(ServiceError::UnsupportedEndpoint)
        );
    }
}
