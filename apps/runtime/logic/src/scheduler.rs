//! Runtime ownership of schedule policy and worker coordination.
#![allow(
    missing_docs,
    reason = "logic-local schedule records are exhaustively mapped at the layer boundary"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the schedule logic port exposes one documented closed error taxonomy"
)]

use agentmod_primitives::SessionId;
use agentmod_runtime_data::scheduler::{
    RuntimeScheduleDataError, RuntimeScheduleDataPort, RuntimeScheduleDataRecord,
    ScheduleDataPayload, ScheduleDataTrigger, ScheduledExecutionDataRecord,
};
use thiserror::Error;

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
pub struct UpsertScheduleCommand {
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
pub struct RuntimeSchedule {
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
pub struct ScheduledExecution {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub schedule: RuntimeSchedule,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleStoreResult {
    pub schedule_id: String,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FireRuntimeEventCommand {
    pub event_id: String,
    pub event_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FireProcessOutputCommand {
    pub output_id: String,
    pub process_id: String,
    pub output: String,
}

pub trait RuntimeScheduleLogicPort: Send + Sync {
    fn upsert_schedule(
        &self,
        command: UpsertScheduleCommand,
    ) -> Result<ScheduleStoreResult, RuntimeScheduleLogicError>;
    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, RuntimeScheduleLogicError>;
    fn list_schedules(&self, limit: u32)
    -> Result<Vec<RuntimeSchedule>, RuntimeScheduleLogicError>;
    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError>;
    fn fire_runtime_event(
        &self,
        command: FireRuntimeEventCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError>;
    fn fire_process_output(
        &self,
        command: FireProcessOutputCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError>;
    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeScheduleLogicError>;
}

impl<D: RuntimeScheduleDataPort> RuntimeScheduleLogicPort for crate::RuntimeLogic<D> {
    fn upsert_schedule(
        &self,
        command: UpsertScheduleCommand,
    ) -> Result<ScheduleStoreResult, RuntimeScheduleLogicError> {
        validate_schedule(&command)?;
        self.data
            .upsert_schedule(to_data(command))
            .map(|value| ScheduleStoreResult {
                schedule_id: value.schedule_id,
                replayed: value.replayed,
            })
            .map_err(RuntimeScheduleLogicError::Data)
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, RuntimeScheduleLogicError> {
        validate_id(schedule_id)?;
        self.data
            .remove_schedule(schedule_id)
            .map_err(RuntimeScheduleLogicError::Data)
    }

    fn list_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<RuntimeSchedule>, RuntimeScheduleLogicError> {
        validate_limit(limit)?;
        self.data
            .list_schedules(limit)
            .map(|values| {
                values
                    .into_iter()
                    .map(from_data_schedule)
                    .collect::<Result<_, _>>()
            })
            .map_err(RuntimeScheduleLogicError::Data)?
    }

    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        validate_limit(limit)?;
        self.data
            .claim_due_schedules(limit)
            .map_err(RuntimeScheduleLogicError::Data)?
            .into_iter()
            .map(from_data_execution)
            .collect()
    }

    fn fire_runtime_event(
        &self,
        command: FireRuntimeEventCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        validate_id(&command.event_id)?;
        validate_text(&command.event_type, 256)?;
        self.data
            .fire_runtime_event(&command.event_id, &command.event_type)
            .map_err(RuntimeScheduleLogicError::Data)?
            .into_iter()
            .map(from_data_execution)
            .collect()
    }

    fn fire_process_output(
        &self,
        command: FireProcessOutputCommand,
    ) -> Result<Vec<ScheduledExecution>, RuntimeScheduleLogicError> {
        validate_id(&command.output_id)?;
        validate_id(&command.process_id)?;
        if command.output.len() > 64 * 1024 {
            return Err(RuntimeScheduleLogicError::Invalid);
        }
        self.data
            .fire_process_output(&command.output_id, &command.process_id, &command.output)
            .map_err(RuntimeScheduleLogicError::Data)?
            .into_iter()
            .map(from_data_execution)
            .collect()
    }

    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeScheduleLogicError> {
        validate_hash(execution_id)?;
        self.data
            .complete_scheduled_execution(execution_id, succeeded)
            .map_err(RuntimeScheduleLogicError::Data)
    }
}

fn validate_schedule(value: &UpsertScheduleCommand) -> Result<(), RuntimeScheduleLogicError> {
    validate_id(&value.schedule_id)?;
    validate_id(&value.idempotency_id)?;
    for text in [
        &value.style,
        &value.workspace,
        &value.permission_policy,
        &value.provider,
        &value.model,
    ] {
        validate_text(text, 4096)?;
    }
    match &value.trigger {
        ScheduleTrigger::AtMillis(value) if *value >= 0 => {}
        ScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        } if *starts_at_ms >= 0 && *every_ms >= 1_000 => {}
        ScheduleTrigger::RuntimeEvent { event_type } => validate_text(event_type, 256)?,
        ScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => {
            validate_id(process_id)?;
            validate_text(contains, 4096)?;
        }
        ScheduleTrigger::AtMillis(_) | ScheduleTrigger::Interval { .. } => {
            return Err(RuntimeScheduleLogicError::Invalid);
        }
    }
    match &value.payload {
        SchedulePayload::Prompt { prompt } => validate_text(prompt, 256 * 1024)?,
        SchedulePayload::Continuation { continuation_id } => validate_id(continuation_id)?,
    }
    Ok(())
}

fn validate_limit(limit: u32) -> Result<(), RuntimeScheduleLogicError> {
    if (1..=1_000).contains(&limit) {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn validate_id(value: &str) -> Result<(), RuntimeScheduleLogicError> {
    if !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn validate_hash(value: &str) -> Result<(), RuntimeScheduleLogicError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn validate_text(value: &str, maximum: usize) -> Result<(), RuntimeScheduleLogicError> {
    if !value.trim().is_empty() && value.len() <= maximum && !value.contains('\0') {
        Ok(())
    } else {
        Err(RuntimeScheduleLogicError::Invalid)
    }
}

fn to_data(value: UpsertScheduleCommand) -> RuntimeScheduleDataRecord {
    RuntimeScheduleDataRecord {
        schedule_id: value.schedule_id,
        session_id: value.session_id.to_string(),
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

fn from_data_schedule(
    value: RuntimeScheduleDataRecord,
) -> Result<RuntimeSchedule, RuntimeScheduleLogicError> {
    Ok(RuntimeSchedule {
        schedule_id: value.schedule_id,
        session_id: value
            .session_id
            .parse()
            .map_err(|_| RuntimeScheduleLogicError::CorruptData)?,
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
    })
}

fn from_data_execution(
    value: ScheduledExecutionDataRecord,
) -> Result<ScheduledExecution, RuntimeScheduleLogicError> {
    Ok(ScheduledExecution {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        schedule: from_data_schedule(value.schedule)?,
    })
}

#[derive(Debug, Eq, Error, PartialEq)]
pub enum RuntimeScheduleLogicError {
    #[error("invalid schedule request")]
    Invalid,
    #[error("scheduler data is unavailable: {0}")]
    Data(#[source] RuntimeScheduleDataError),
    #[error("scheduler returned corrupt business data")]
    CorruptData,
}
