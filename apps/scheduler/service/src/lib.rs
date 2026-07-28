//! Versioned scheduler endpoints and explicit wire-to-logic mapping.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use agentmod_scheduler_logic::{
    ExecutionResult, ScheduleCommand, SchedulePayload, ScheduleResult, ScheduleTrigger,
    SchedulerLogicError, SchedulerLogicPort,
};
use agentmod_scheduler_protocol::{
    CURRENT_PROTOCOL_VERSION, SchedulePayload as WirePayload, ScheduleSpec,
    ScheduleTrigger as WireTrigger, ScheduledExecution, SchedulerCommand, SchedulerResponse,
};
use thiserror::Error;

/// Scheduler protocol endpoint.
#[derive(Clone)]
pub struct SchedulerService<L> {
    logic: L,
    negotiated: Arc<AtomicBool>,
    authentication_token: Arc<Vec<u8>>,
}

impl<L> SchedulerService<L> {
    /// Injects logic.
    ///
    /// # Errors
    ///
    /// Rejects a bootstrap token shorter than 32 bytes.
    pub fn new(logic: L, authentication_token: String) -> Result<Self, SchedulerServiceError> {
        if authentication_token.len() < 32 {
            return Err(SchedulerServiceError::InvalidAuthentication);
        }
        Ok(Self {
            logic,
            negotiated: Arc::new(AtomicBool::new(false)),
            authentication_token: Arc::new(authentication_token.into_bytes()),
        })
    }
}

impl<L: SchedulerLogicPort> SchedulerService<L> {
    /// Handles one versioned command.
    pub fn handle(&self, command: SchedulerCommand) -> SchedulerResponse {
        if let SchedulerCommand::Negotiate {
            protocol_version,
            capabilities: runtime_capabilities,
            authentication_token,
        } = command
        {
            if protocol_version != CURRENT_PROTOCOL_VERSION
                || !runtime_capabilities
                    .iter()
                    .any(|value| value == "durable_schedules")
                || !constant_time_eq(authentication_token.as_bytes(), &self.authentication_token)
            {
                return SchedulerResponse::Error {
                    code: "incompatible_protocol".to_owned(),
                    message: "scheduler protocol or required capability is incompatible".to_owned(),
                };
            }
            self.negotiated.store(true, Ordering::Release);
            return SchedulerResponse::Negotiated {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                capabilities: capabilities(),
            };
        }
        if !self.negotiated.load(Ordering::Acquire) {
            return SchedulerResponse::Error {
                code: "negotiation_required".to_owned(),
                message: "scheduler negotiation must complete first".to_owned(),
            };
        }
        match command {
            SchedulerCommand::Negotiate { .. } => unreachable!("handled above"),
            SchedulerCommand::Upsert { schedule } => self
                .logic
                .upsert(to_logic(*schedule))
                .map_or_else(error, |value| SchedulerResponse::Stored {
                    schedule_id: value.schedule_id,
                    replayed: value.replayed,
                }),
            SchedulerCommand::Remove { schedule_id } => self
                .logic
                .remove(&schedule_id)
                .map_or_else(error, |existed| SchedulerResponse::Removed { existed }),
            SchedulerCommand::List { limit } => {
                self.logic.list(limit).map_or_else(error, |schedules| {
                    SchedulerResponse::Schedules {
                        schedules: schedules.into_iter().map(to_wire_schedule).collect(),
                    }
                })
            }
            SchedulerCommand::ClaimDue { limit } => {
                self.logic.claim_due(limit).map_or_else(error, executions)
            }
            SchedulerCommand::FireRuntimeEvent {
                event_id,
                event_type,
            } => self
                .logic
                .fire_runtime_event(&event_id, &event_type)
                .map_or_else(error, executions),
            SchedulerCommand::FireProcessOutput {
                output_id,
                process_id,
                output,
            } => self
                .logic
                .fire_process_output(&output_id, &process_id, &output)
                .map_or_else(error, executions),
            SchedulerCommand::CompleteExecution {
                execution_id,
                succeeded,
            } => self
                .logic
                .complete_execution(&execution_id, succeeded)
                .map_or_else(error, |changed| SchedulerResponse::ExecutionCompleted {
                    changed,
                }),
            SchedulerCommand::Health => {
                self.logic
                    .health()
                    .map_or_else(error, |()| SchedulerResponse::Health {
                        status: "ok".to_owned(),
                    })
            }
        }
    }
}

fn executions(values: Vec<ExecutionResult>) -> SchedulerResponse {
    SchedulerResponse::Executions {
        executions: values
            .into_iter()
            .map(|value| ScheduledExecution {
                execution_id: value.execution_id,
                scheduled_for_ms: value.scheduled_for_ms,
                claimed_at_ms: value.claimed_at_ms,
                schedule: to_wire_schedule(value.schedule),
            })
            .collect(),
    }
}

fn to_logic(value: ScheduleSpec) -> ScheduleCommand {
    ScheduleCommand {
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
            WireTrigger::AtMillis(at) => ScheduleTrigger::AtMillis(at),
            WireTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            WireTrigger::RuntimeEvent { event_type } => {
                ScheduleTrigger::RuntimeEvent { event_type }
            }
            WireTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            WirePayload::Prompt { prompt } => SchedulePayload::Prompt { prompt },
            WirePayload::Continuation { continuation_id } => {
                SchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn to_wire_schedule(value: ScheduleResult) -> ScheduleSpec {
    ScheduleSpec {
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
            ScheduleTrigger::AtMillis(at) => WireTrigger::AtMillis(at),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => WireTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleTrigger::RuntimeEvent { event_type } => {
                WireTrigger::RuntimeEvent { event_type }
            }
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => WireTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            SchedulePayload::Prompt { prompt } => WirePayload::Prompt { prompt },
            SchedulePayload::Continuation { continuation_id } => {
                WirePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "logic errors are deliberately consumed at the service boundary"
)]
fn error(value: SchedulerLogicError) -> SchedulerResponse {
    let code = match value {
        SchedulerLogicError::Invalid => "invalid_request",
        SchedulerLogicError::IdempotencyConflict => "idempotency_conflict",
        SchedulerLogicError::TerminalConflict => "terminal_conflict",
        SchedulerLogicError::NotFound => "not_found",
        SchedulerLogicError::Unavailable => "unavailable",
    };
    SchedulerResponse::Error {
        code: code.to_owned(),
        message: value.to_string(),
    }
}

fn capabilities() -> Vec<String> {
    [
        "background_prompt",
        "deferred_continuation",
        "durable_schedules",
        "process_output_trigger",
        "recurring",
        "runtime_event_trigger",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let maximum = left.len().max(right.len());
    for index in 0..maximum {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

/// Bootstrap configuration failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SchedulerServiceError {
    /// Token is too short to authenticate a local peer.
    #[error("scheduler authentication token must contain at least 32 bytes")]
    InvalidAuthentication,
}

#[cfg(test)]
mod tests {
    use agentmod_scheduler_logic::{
        ExecutionResult, ScheduleCommand, ScheduleResult, SchedulerLogicError, SchedulerLogicPort,
        StoreResult,
    };
    use agentmod_scheduler_protocol::{SchedulerCommand, SchedulerResponse};

    use super::SchedulerService;

    #[derive(Clone)]
    struct MockLogic;

    impl SchedulerLogicPort for MockLogic {
        fn upsert(&self, _: ScheduleCommand) -> Result<StoreResult, SchedulerLogicError> {
            unreachable!()
        }
        fn remove(&self, _: &str) -> Result<bool, SchedulerLogicError> {
            unreachable!()
        }
        fn list(&self, _: u32) -> Result<Vec<ScheduleResult>, SchedulerLogicError> {
            unreachable!()
        }
        fn claim_due(&self, _: u32) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
            unreachable!()
        }
        fn fire_runtime_event(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
            unreachable!()
        }
        fn fire_process_output(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Vec<ExecutionResult>, SchedulerLogicError> {
            unreachable!()
        }
        fn complete_execution(&self, _: &str, _: bool) -> Result<bool, SchedulerLogicError> {
            unreachable!()
        }
        fn health(&self) -> Result<(), SchedulerLogicError> {
            Ok(())
        }
    }

    #[test]
    fn service_requires_compatible_negotiation_before_logic() {
        let service = SchedulerService::new(MockLogic, "a".repeat(32)).expect("service");
        assert!(matches!(
            service.handle(SchedulerCommand::Health),
            SchedulerResponse::Error { ref code, .. } if code == "negotiation_required"
        ));
        assert!(matches!(
            service.handle(SchedulerCommand::Negotiate {
                protocol_version: 1,
                capabilities: vec!["durable_schedules".to_owned()],
                authentication_token: "a".repeat(32),
            }),
            SchedulerResponse::Negotiated {
                protocol_version: 1,
                ..
            }
        ));
        assert_eq!(
            service.handle(SchedulerCommand::Health),
            SchedulerResponse::Health {
                status: "ok".to_owned()
            }
        );
    }

    #[test]
    fn service_returns_readable_incompatibility() {
        let service = SchedulerService::new(MockLogic, "a".repeat(32)).expect("service");
        assert!(matches!(
            service.handle(SchedulerCommand::Negotiate {
                protocol_version: 1,
                capabilities: vec!["durable_schedules".to_owned()],
                authentication_token: "b".repeat(32),
            }),
            SchedulerResponse::Error { ref code, .. } if code == "incompatible_protocol"
        ));
    }
}
