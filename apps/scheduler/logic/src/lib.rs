//! Schedule validation, trigger matching, budgets, and execution semantics.
#![allow(
    missing_docs,
    reason = "logic-owned schedule commands are exhaustively named and architecture-documented"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the logic port exposes one documented closed error taxonomy"
)]

use agentmod_scheduler_data::{
    DataPayload, DataTrigger, ExecutionDataRecord, ScheduleDataRecord, SchedulerDataError,
    SchedulerDataPort,
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
pub struct ScheduleCommand {
    pub schedule_id: String,
    pub session_id: String,
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
    pub session_id: String,
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
pub struct ExecutionResult {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub claimed_at_ms: i64,
    pub schedule: ScheduleResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreResult {
    pub schedule_id: String,
    pub replayed: bool,
}

pub trait SchedulerLogicPort: Send + Sync {
    fn upsert(&self, schedule: ScheduleCommand) -> Result<StoreResult, SchedulerLogicError>;
    fn remove(&self, schedule_id: &str) -> Result<bool, SchedulerLogicError>;
    fn list(&self, limit: u32) -> Result<Vec<ScheduleResult>, SchedulerLogicError>;
    fn claim_due(&self, limit: u32) -> Result<Vec<ExecutionResult>, SchedulerLogicError>;
    fn list_pending_executions(
        &self,
        _limit: u32,
    ) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
        Err(SchedulerLogicError::Unavailable)
    }
    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<ExecutionResult>, SchedulerLogicError>;
    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<ExecutionResult>, SchedulerLogicError>;
    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, SchedulerLogicError>;
    fn health(&self) -> Result<(), SchedulerLogicError>;
}

#[derive(Clone)]
pub struct SchedulerLogic<D> {
    data: D,
}

impl<D> SchedulerLogic<D> {
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D: SchedulerDataPort> SchedulerLogicPort for SchedulerLogic<D> {
    fn upsert(&self, schedule: ScheduleCommand) -> Result<StoreResult, SchedulerLogicError> {
        validate_schedule(&schedule)?;
        self.data
            .upsert(to_data(schedule))
            .map(|value| StoreResult {
                schedule_id: value.schedule_id,
                replayed: value.replayed,
            })
            .map_err(map_error)
    }

    fn remove(&self, schedule_id: &str) -> Result<bool, SchedulerLogicError> {
        validate_id(schedule_id)?;
        self.data.remove(schedule_id).map_err(map_error)
    }

    fn list(&self, limit: u32) -> Result<Vec<ScheduleResult>, SchedulerLogicError> {
        let limit = validate_limit(limit)?;
        self.data
            .list(limit)
            .map(|values| values.into_iter().map(from_schedule).collect())
            .map_err(map_error)
    }

    fn claim_due(&self, limit: u32) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
        let limit = validate_limit(limit)?;
        self.data
            .claim_due(limit)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(map_error)
    }

    fn list_pending_executions(
        &self,
        limit: u32,
    ) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
        let limit = validate_limit(limit)?;
        self.data
            .list_pending_executions(limit)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(map_error)
    }

    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
        validate_id(event_id)?;
        validate_text(event_type, 256)?;
        self.data
            .fire_runtime_event(event_id, event_type)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(map_error)
    }

    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
        validate_id(output_id)?;
        validate_id(process_id)?;
        if output.len() > 64 * 1024 {
            return Err(SchedulerLogicError::Invalid);
        }
        self.data
            .fire_process_output(output_id, process_id, output)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(map_error)
    }

    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, SchedulerLogicError> {
        if execution_id.len() != 64 || !execution_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(SchedulerLogicError::Invalid);
        }
        self.data
            .complete_execution(execution_id, succeeded)
            .map_err(map_error)
    }

    fn health(&self) -> Result<(), SchedulerLogicError> {
        self.data.health().map_err(map_error)
    }
}

fn validate_schedule(value: &ScheduleCommand) -> Result<(), SchedulerLogicError> {
    validate_id(&value.schedule_id)?;
    validate_id(&value.session_id)?;
    validate_id(&value.idempotency_id)?;
    validate_text(&value.style, 128)?;
    validate_text(&value.workspace, 16 * 1024)?;
    validate_text(&value.permission_policy, 128)?;
    validate_text(&value.provider, 128)?;
    validate_text(&value.model, 256)?;
    if value.token_budget == 0 {
        return Err(SchedulerLogicError::Invalid);
    }
    match &value.trigger {
        ScheduleTrigger::Interval { every_ms, .. } if *every_ms < 1_000 => {
            return Err(SchedulerLogicError::Invalid);
        }
        ScheduleTrigger::RuntimeEvent { event_type } => validate_text(event_type, 256)?,
        ScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => {
            validate_id(process_id)?;
            validate_text(contains, 4096)?;
        }
        ScheduleTrigger::AtMillis(_) | ScheduleTrigger::Interval { .. } => {}
    }
    match &value.payload {
        SchedulePayload::Prompt { prompt } => validate_text(prompt, 256 * 1024),
        SchedulePayload::Continuation { continuation_id } => validate_id(continuation_id),
    }
}

fn validate_limit(value: u32) -> Result<usize, SchedulerLogicError> {
    if value == 0 || value > 1_000 {
        Err(SchedulerLogicError::Invalid)
    } else {
        usize::try_from(value).map_err(|_| SchedulerLogicError::Invalid)
    }
}

fn validate_id(value: &str) -> Result<(), SchedulerLogicError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'))
    {
        Err(SchedulerLogicError::Invalid)
    } else {
        Ok(())
    }
}

fn validate_text(value: &str, maximum: usize) -> Result<(), SchedulerLogicError> {
    if value.trim().is_empty() || value.len() > maximum || value.contains('\0') {
        Err(SchedulerLogicError::Invalid)
    } else {
        Ok(())
    }
}

fn to_data(value: ScheduleCommand) -> ScheduleDataRecord {
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
            ScheduleTrigger::AtMillis(at) => DataTrigger::AtMillis(at),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => DataTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleTrigger::RuntimeEvent { event_type } => {
                DataTrigger::RuntimeEvent { event_type }
            }
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => DataTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            SchedulePayload::Prompt { prompt } => DataPayload::Prompt { prompt },
            SchedulePayload::Continuation { continuation_id } => {
                DataPayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_schedule(value: ScheduleDataRecord) -> ScheduleResult {
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
            DataTrigger::AtMillis(at) => ScheduleTrigger::AtMillis(at),
            DataTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            DataTrigger::RuntimeEvent { event_type } => {
                ScheduleTrigger::RuntimeEvent { event_type }
            }
            DataTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            DataPayload::Prompt { prompt } => SchedulePayload::Prompt { prompt },
            DataPayload::Continuation { continuation_id } => {
                SchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_execution(value: ExecutionDataRecord) -> ExecutionResult {
    ExecutionResult {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        claimed_at_ms: value.claimed_at_ms,
        schedule: from_schedule(value.schedule),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "data errors are deliberately consumed at the logic boundary"
)]
const fn map_error(value: SchedulerDataError) -> SchedulerLogicError {
    match value {
        SchedulerDataError::Invalid => SchedulerLogicError::Invalid,
        SchedulerDataError::IdempotencyConflict => SchedulerLogicError::IdempotencyConflict,
        SchedulerDataError::TerminalConflict => SchedulerLogicError::TerminalConflict,
        SchedulerDataError::NotFound => SchedulerLogicError::NotFound,
        SchedulerDataError::Corrupt | SchedulerDataError::Unavailable => {
            SchedulerLogicError::Unavailable
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SchedulerLogicError {
    #[error("invalid scheduler command")]
    Invalid,
    #[error("scheduler idempotency conflict")]
    IdempotencyConflict,
    #[error("scheduler execution terminal conflict")]
    TerminalConflict,
    #[error("scheduler record not found")]
    NotFound,
    #[error("scheduler unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use agentmod_scheduler_data::{
        ExecutionDataRecord, ScheduleDataRecord, SchedulerDataError, SchedulerDataPort,
        StoreDataResult,
    };

    use super::{
        ScheduleCommand, SchedulePayload, ScheduleTrigger, SchedulerLogic, SchedulerLogicError,
        SchedulerLogicPort,
    };

    #[derive(Clone)]
    struct MockData;

    impl SchedulerDataPort for MockData {
        fn upsert(&self, value: ScheduleDataRecord) -> Result<StoreDataResult, SchedulerDataError> {
            Ok(StoreDataResult {
                schedule_id: value.schedule_id,
                replayed: false,
            })
        }
        fn remove(&self, _: &str) -> Result<bool, SchedulerDataError> {
            Ok(false)
        }
        fn list(&self, _: usize) -> Result<Vec<ScheduleDataRecord>, SchedulerDataError> {
            Ok(Vec::new())
        }
        fn claim_due(&self, _: usize) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError> {
            Ok(Vec::new())
        }
        fn fire_runtime_event(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError> {
            Ok(Vec::new())
        }
        fn fire_process_output(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError> {
            Ok(Vec::new())
        }
        fn complete_execution(&self, _: &str, _: bool) -> Result<bool, SchedulerDataError> {
            Ok(false)
        }
        fn health(&self) -> Result<(), SchedulerDataError> {
            Ok(())
        }
    }

    #[test]
    fn logic_enforces_minimum_recurring_interval_before_data() {
        let logic = SchedulerLogic::new(MockData);
        let mut value = fixture();
        value.trigger = ScheduleTrigger::Interval {
            starts_at_ms: 0,
            every_ms: 999,
        };
        assert_eq!(logic.upsert(value), Err(SchedulerLogicError::Invalid));
    }

    #[test]
    fn logic_maps_valid_schedule_to_data() {
        let result = SchedulerLogic::new(MockData)
            .upsert(fixture())
            .expect("store");
        assert_eq!(result.schedule_id, "schedule");
    }

    fn fixture() -> ScheduleCommand {
        ScheduleCommand {
            schedule_id: "schedule".to_owned(),
            session_id: "session".to_owned(),
            idempotency_id: "idempotency".to_owned(),
            style: "persistent-chat".to_owned(),
            workspace: "workspace".to_owned(),
            permission_policy: "safe".to_owned(),
            provider: "mock".to_owned(),
            model: "mock".to_owned(),
            token_budget: 1,
            cost_budget_micros: 0,
            trigger: ScheduleTrigger::AtMillis(0),
            payload: SchedulePayload::Prompt {
                prompt: "work".to_owned(),
            },
            active: true,
        }
    }
}
