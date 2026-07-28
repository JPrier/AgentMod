//! Runtime business datasets for the isolated scheduler worker.
#![allow(
    missing_docs,
    reason = "data-local schedule records are exhaustively mapped at the layer boundary"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the schedule data port exposes one documented closed error taxonomy"
)]

use agentmod_runtime_dependency::scheduler::{
    DependencyRuntimeSchedule, DependencySchedulePayload, DependencyScheduleStoreResult,
    DependencyScheduleTrigger, DependencyScheduledExecution, RuntimeSchedulerDependencyError,
    RuntimeSchedulerDependencyPort,
};
use thiserror::Error;

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
pub struct RuntimeScheduleDataRecord {
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
    pub trigger: ScheduleDataTrigger,
    pub payload: ScheduleDataPayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledExecutionDataRecord {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub schedule: RuntimeScheduleDataRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleStoreDataResult {
    pub schedule_id: String,
    pub replayed: bool,
}

pub trait RuntimeScheduleDataPort: Send + Sync {
    fn upsert_schedule(
        &self,
        schedule: RuntimeScheduleDataRecord,
    ) -> Result<ScheduleStoreDataResult, RuntimeScheduleDataError>;
    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, RuntimeScheduleDataError>;
    fn list_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<RuntimeScheduleDataRecord>, RuntimeScheduleDataError>;
    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, RuntimeScheduleDataError>;
    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, RuntimeScheduleDataError>;
    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, RuntimeScheduleDataError>;
    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeScheduleDataError>;
}

impl<D: RuntimeSchedulerDependencyPort> RuntimeScheduleDataPort for crate::RuntimeData<D> {
    fn upsert_schedule(
        &self,
        schedule: RuntimeScheduleDataRecord,
    ) -> Result<ScheduleStoreDataResult, RuntimeScheduleDataError> {
        self.dependency
            .upsert(to_dependency_schedule(schedule))
            .map(from_store_result)
            .map_err(RuntimeScheduleDataError::Dependency)
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, RuntimeScheduleDataError> {
        self.dependency
            .remove(schedule_id)
            .map_err(RuntimeScheduleDataError::Dependency)
    }

    fn list_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<RuntimeScheduleDataRecord>, RuntimeScheduleDataError> {
        self.dependency
            .list(limit)
            .map(|values| values.into_iter().map(from_dependency_schedule).collect())
            .map_err(RuntimeScheduleDataError::Dependency)
    }

    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, RuntimeScheduleDataError> {
        self.dependency
            .claim_due(limit)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(RuntimeScheduleDataError::Dependency)
    }

    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, RuntimeScheduleDataError> {
        self.dependency
            .fire_runtime_event(event_id, event_type)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(RuntimeScheduleDataError::Dependency)
    }

    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<ScheduledExecutionDataRecord>, RuntimeScheduleDataError> {
        self.dependency
            .fire_process_output(output_id, process_id, output)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(RuntimeScheduleDataError::Dependency)
    }

    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeScheduleDataError> {
        self.dependency
            .complete_execution(execution_id, succeeded)
            .map_err(RuntimeScheduleDataError::Dependency)
    }
}

fn to_dependency_schedule(value: RuntimeScheduleDataRecord) -> DependencyRuntimeSchedule {
    DependencyRuntimeSchedule {
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
        },
        payload: match value.payload {
            ScheduleDataPayload::Prompt { prompt } => DependencySchedulePayload::Prompt { prompt },
            ScheduleDataPayload::Continuation { continuation_id } => {
                DependencySchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_dependency_schedule(value: DependencyRuntimeSchedule) -> RuntimeScheduleDataRecord {
    RuntimeScheduleDataRecord {
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

fn from_execution(value: DependencyScheduledExecution) -> ScheduledExecutionDataRecord {
    ScheduledExecutionDataRecord {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        schedule: from_dependency_schedule(value.schedule),
    }
}

fn from_store_result(value: DependencyScheduleStoreResult) -> ScheduleStoreDataResult {
    ScheduleStoreDataResult {
        schedule_id: value.schedule_id,
        replayed: value.replayed,
    }
}

#[derive(Debug, Eq, Error, PartialEq)]
pub enum RuntimeScheduleDataError {
    #[error("scheduler dependency failed: {0}")]
    Dependency(#[source] RuntimeSchedulerDependencyError),
}
