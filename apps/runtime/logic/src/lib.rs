//! Runtime business logic.

pub mod action;
pub mod artifact;
pub mod canonical_variable_coordinator;
pub mod canonical_variables;
pub mod child_graph_ancillary_policy;
pub mod child_graph_application;
pub mod child_graph_continuation;
pub mod child_graph_execution;
pub mod child_graph_turn;
pub mod child_message_execution;
pub mod child_session;
pub mod compaction;
pub mod continuation;
pub mod conversation;
pub mod effect_output;
pub mod execution_plan;
pub mod harness;
pub mod harness_registry;
pub mod history;
pub mod information_flow;
pub mod interception;
pub mod introspection;
pub mod mcp_oauth;
pub mod memory;
pub mod node_execution;
pub mod node_executor;
pub mod parallel_driver;
pub mod parallel_execution;
pub mod parallel_turn;
pub mod permission;
pub mod persistence;
pub mod plugin;
pub mod plugin_authorization;
pub mod plugin_automatic_memory;
pub mod plugin_context_operation;
pub mod plugin_context_turn;
pub mod plugin_lifecycle;
pub mod plugin_observer;
pub mod plugin_outcome;
pub mod plugin_schema;
pub mod plugin_state_turn;
pub mod plugin_turn;
pub(crate) mod projection;
pub mod registry;
pub mod scheduler;
pub(crate) mod scoped_task;
pub mod session;
pub mod style;
pub(crate) mod style_executor;
pub mod tool;
pub mod turn;
pub mod variable_turn;
pub mod workspace;

use std::path::PathBuf;

use agentmod_runtime_data::{DataError, RuntimeDataPort, RuntimeHealthDataRequest};
use thiserror::Error;

/// Logic-owned health command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetRuntimeHealthCommand {
    /// Canonical session storage selected by validated runtime configuration.
    pub canonical_session_root: PathBuf,
}

/// Logic-owned runtime health state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealthResult {
    /// Business status derived from required runtime datasets.
    pub state: RuntimeHealthState,
    /// Safe diagnostic labels, excluding private content.
    pub diagnostics: Vec<String>,
}

/// Business-level runtime health state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeHealthState {
    /// Required business datasets are usable.
    Ready,
    /// Runtime can answer but a required dataset is unavailable.
    Degraded,
}

/// Narrow logic interface consumed by runtime service.
pub trait RuntimeLogicPort {
    /// Evaluates runtime health business semantics.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] when the required health dataset cannot be built.
    fn get_health(
        &self,
        command: GetRuntimeHealthCommand,
    ) -> Result<RuntimeHealthResult, LogicError>;
}

/// Runtime business implementation over an injected data interface.
#[derive(Clone, Debug)]
pub struct RuntimeLogic<D> {
    data: D,
}

impl<D> RuntimeLogic<D> {
    /// Creates logic using only a runtime data implementation.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D> RuntimeLogicPort for RuntimeLogic<D>
where
    D: RuntimeDataPort,
{
    fn get_health(
        &self,
        command: GetRuntimeHealthCommand,
    ) -> Result<RuntimeHealthResult, LogicError> {
        let record = self
            .data
            .runtime_health(RuntimeHealthDataRequest {
                session_storage_root: command.canonical_session_root,
            })
            .map_err(LogicError::HealthData)?;
        let state = if record.canonical_storage_available {
            RuntimeHealthState::Ready
        } else {
            RuntimeHealthState::Degraded
        };
        Ok(RuntimeHealthResult {
            state,
            diagnostics: vec![format!("canonical storage: {}", record.storage_label)],
        })
    }
}

/// Runtime business failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum LogicError {
    /// Required health dataset could not be constructed.
    #[error("runtime health data unavailable: {0}")]
    HealthData(DataError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_data::RuntimeHealthDataRecord;

    use super::*;

    struct MockData {
        available: bool,
        observed: RefCell<Vec<RuntimeHealthDataRequest>>,
    }

    impl RuntimeDataPort for MockData {
        fn runtime_health(
            &self,
            request: RuntimeHealthDataRequest,
        ) -> Result<RuntimeHealthDataRecord, DataError> {
            self.observed.borrow_mut().push(request);
            Ok(RuntimeHealthDataRecord {
                canonical_storage_available: self.available,
                storage_label: "fixture".into(),
            })
        }
    }

    #[test]
    fn ready_requires_available_canonical_storage() {
        let logic = RuntimeLogic::new(MockData {
            available: true,
            observed: RefCell::new(Vec::new()),
        });
        let result = logic
            .get_health(GetRuntimeHealthCommand {
                canonical_session_root: PathBuf::from("sessions"),
            })
            .expect("health");
        assert_eq!(result.state, RuntimeHealthState::Ready);
        assert_eq!(
            logic.data.observed.into_inner(),
            vec![RuntimeHealthDataRequest {
                session_storage_root: PathBuf::from("sessions")
            }]
        );
    }

    #[test]
    fn unavailable_storage_is_degraded_not_ready() {
        let logic = RuntimeLogic::new(MockData {
            available: false,
            observed: RefCell::new(Vec::new()),
        });
        assert_eq!(
            logic
                .get_health(GetRuntimeHealthCommand {
                    canonical_session_root: PathBuf::from("sessions"),
                })
                .expect("health")
                .state,
            RuntimeHealthState::Degraded
        );
    }
}
