//! Business-facing scheduler datasets and dependency normalization.
#![allow(
    missing_docs,
    reason = "data-owned schedule records are exhaustively named and architecture-documented"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the data port exposes one documented closed error taxonomy"
)]

use agentmod_scheduler_dependency::{
    DependencyExecution, DependencyPayload, DependencySchedule, DependencyStoreResult,
    DependencyTrigger, SchedulerDependencyError, SchedulerDependencyPort,
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataTrigger {
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
pub enum DataPayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleDataRecord {
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
    pub trigger: DataTrigger,
    pub payload: DataPayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDataRecord {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub schedule: ScheduleDataRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreDataResult {
    pub schedule_id: String,
    pub replayed: bool,
}

pub trait SchedulerDataPort: Send + Sync {
    fn upsert(&self, schedule: ScheduleDataRecord) -> Result<StoreDataResult, SchedulerDataError>;
    fn remove(&self, schedule_id: &str) -> Result<bool, SchedulerDataError>;
    fn list(&self, limit: usize) -> Result<Vec<ScheduleDataRecord>, SchedulerDataError>;
    fn claim_due(&self, limit: usize) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError>;
    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError>;
    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError>;
    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, SchedulerDataError>;
    fn health(&self) -> Result<(), SchedulerDataError>;
}

#[derive(Clone)]
pub struct SchedulerData<D> {
    dependency: D,
}

impl<D> SchedulerData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D: SchedulerDependencyPort> SchedulerDataPort for SchedulerData<D> {
    fn upsert(&self, schedule: ScheduleDataRecord) -> Result<StoreDataResult, SchedulerDataError> {
        self.dependency
            .upsert(to_dependency_schedule(schedule))
            .map(from_store)
            .map_err(map_error)
    }

    fn remove(&self, schedule_id: &str) -> Result<bool, SchedulerDataError> {
        self.dependency.remove(schedule_id).map_err(map_error)
    }

    fn list(&self, limit: usize) -> Result<Vec<ScheduleDataRecord>, SchedulerDataError> {
        self.dependency
            .list(limit)
            .map(|values| values.into_iter().map(from_schedule).collect())
            .map_err(map_error)
    }

    fn claim_due(&self, limit: usize) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError> {
        self.dependency
            .claim_due(limit)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(map_error)
    }

    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError> {
        self.dependency
            .fire_runtime_event(event_id, event_type)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(map_error)
    }

    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<ExecutionDataRecord>, SchedulerDataError> {
        self.dependency
            .fire_process_output(output_id, process_id, output)
            .map(|values| values.into_iter().map(from_execution).collect())
            .map_err(map_error)
    }

    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, SchedulerDataError> {
        self.dependency
            .complete_execution(execution_id, succeeded)
            .map_err(map_error)
    }

    fn health(&self) -> Result<(), SchedulerDataError> {
        self.dependency.health().map_err(map_error)
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
        trigger: match value.trigger {
            DataTrigger::AtMillis(at) => DependencyTrigger::AtMillis(at),
            DataTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => DependencyTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            DataTrigger::RuntimeEvent { event_type } => {
                DependencyTrigger::RuntimeEvent { event_type }
            }
            DataTrigger::ProcessOutput {
                process_id,
                contains,
            } => DependencyTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            DataPayload::Prompt { prompt } => DependencyPayload::Prompt { prompt },
            DataPayload::Continuation { continuation_id } => {
                DependencyPayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_schedule(value: DependencySchedule) -> ScheduleDataRecord {
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
            DependencyTrigger::AtMillis(at) => DataTrigger::AtMillis(at),
            DependencyTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => DataTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            DependencyTrigger::RuntimeEvent { event_type } => {
                DataTrigger::RuntimeEvent { event_type }
            }
            DependencyTrigger::ProcessOutput {
                process_id,
                contains,
            } => DataTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            DependencyPayload::Prompt { prompt } => DataPayload::Prompt { prompt },
            DependencyPayload::Continuation { continuation_id } => {
                DataPayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_execution(value: DependencyExecution) -> ExecutionDataRecord {
    ExecutionDataRecord {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        schedule: from_schedule(value.schedule),
    }
}

fn from_store(value: DependencyStoreResult) -> StoreDataResult {
    StoreDataResult {
        schedule_id: value.schedule_id,
        replayed: value.replayed,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "dependency errors are deliberately consumed at the data boundary"
)]
const fn map_error(value: SchedulerDependencyError) -> SchedulerDataError {
    match value {
        SchedulerDependencyError::Configuration | SchedulerDependencyError::Invalid => {
            SchedulerDataError::Invalid
        }
        SchedulerDependencyError::IdempotencyConflict => SchedulerDataError::IdempotencyConflict,
        SchedulerDependencyError::TerminalConflict => SchedulerDataError::TerminalConflict,
        SchedulerDependencyError::NotFound => SchedulerDataError::NotFound,
        SchedulerDependencyError::Corrupt => SchedulerDataError::Corrupt,
        SchedulerDependencyError::Storage | SchedulerDependencyError::Clock => {
            SchedulerDataError::Unavailable
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SchedulerDataError {
    #[error("invalid scheduler data request")]
    Invalid,
    #[error("scheduler idempotency conflict")]
    IdempotencyConflict,
    #[error("scheduler terminal conflict")]
    TerminalConflict,
    #[error("scheduler record not found")]
    NotFound,
    #[error("scheduler record corrupt")]
    Corrupt,
    #[error("scheduler data unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use agentmod_scheduler_dependency::{
        DependencyExecution, DependencySchedule, DependencyStoreResult, SchedulerDependencyError,
        SchedulerDependencyPort,
    };

    use super::{DataPayload, DataTrigger, ScheduleDataRecord, SchedulerData, SchedulerDataPort};

    #[derive(Clone)]
    struct MockDependency;

    impl SchedulerDependencyPort for MockDependency {
        fn upsert(
            &self,
            value: DependencySchedule,
        ) -> Result<DependencyStoreResult, SchedulerDependencyError> {
            assert!(matches!(
                value.trigger,
                agentmod_scheduler_dependency::DependencyTrigger::RuntimeEvent { ref event_type }
                    if event_type == "ready"
            ));
            Ok(DependencyStoreResult {
                schedule_id: value.schedule_id,
                replayed: false,
            })
        }

        fn remove(&self, _: &str) -> Result<bool, SchedulerDependencyError> {
            Ok(false)
        }
        fn list(&self, _: usize) -> Result<Vec<DependencySchedule>, SchedulerDependencyError> {
            Ok(Vec::new())
        }
        fn claim_due(
            &self,
            _: usize,
        ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError> {
            Ok(Vec::new())
        }
        fn fire_runtime_event(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError> {
            Ok(Vec::new())
        }
        fn fire_process_output(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError> {
            Ok(Vec::new())
        }
        fn complete_execution(&self, _: &str, _: bool) -> Result<bool, SchedulerDependencyError> {
            Ok(false)
        }
        fn health(&self) -> Result<(), SchedulerDependencyError> {
            Ok(())
        }
    }

    #[test]
    fn data_maps_layer_owned_schedule_to_dependency() {
        let result = SchedulerData::new(MockDependency)
            .upsert(fixture())
            .expect("store");
        assert_eq!(result.schedule_id, "schedule");
    }

    fn fixture() -> ScheduleDataRecord {
        ScheduleDataRecord {
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
            trigger: DataTrigger::RuntimeEvent {
                event_type: "ready".to_owned(),
            },
            payload: DataPayload::Prompt {
                prompt: "work".to_owned(),
            },
            active: true,
        }
    }
}
