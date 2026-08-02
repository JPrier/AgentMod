//! Data-owned runtime cancellation state.

use std::sync::Arc;

use agentmod_runtime_dependency::cancellation::{
    RuntimeCancellationDependencyError, RuntimeCancellationDependencyPort,
};
use thiserror::Error;

/// Data-owned exact cancellation query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCancellationDataRequest {
    /// Runtime-owned cancellation identity.
    pub cancellation_id: String,
}

/// Data-owned cancellation registration command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestRuntimeCancellationDataCommand {
    /// Runtime-owned cancellation identity.
    pub cancellation_id: String,
}

/// Data-owned cancellation cleanup command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClearRuntimeCancellationDataCommand {
    /// Runtime-owned cancellation identity.
    pub cancellation_id: String,
}

/// Narrow cancellation data port consumed by runtime logic.
pub trait RuntimeCancellationDataPort: Send + Sync {
    /// Reports explicit cancellation state independently of process existence.
    ///
    /// # Errors
    ///
    /// Returns a data-owned error for invalid identity or unavailable state.
    fn cancellation_requested(
        &self,
        request: RuntimeCancellationDataRequest,
    ) -> Result<bool, RuntimeCancellationDataError>;
}

/// Explicit data control port used by runtime cancellation use cases.
pub trait RuntimeCancellationControlDataPort: Send + Sync {
    /// Registers one exact cancellation idempotently.
    ///
    /// # Errors
    ///
    /// Returns a data-owned error for invalid identity, capacity, or
    /// unavailable state.
    fn request_runtime_cancellation(
        &self,
        command: RequestRuntimeCancellationDataCommand,
    ) -> Result<(), RuntimeCancellationDataError>;

    /// Clears one exact terminal cancellation identity.
    ///
    /// # Errors
    ///
    /// Returns a data-owned error for invalid identity or unavailable state.
    fn clear_runtime_cancellation(
        &self,
        command: ClearRuntimeCancellationDataCommand,
    ) -> Result<bool, RuntimeCancellationDataError>;
}

/// Explicit data router over the runtime cancellation source.
#[derive(Clone)]
pub struct RuntimeCancellationData {
    dependency: Arc<dyn RuntimeCancellationDependencyPort>,
}

impl std::fmt::Debug for RuntimeCancellationData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeCancellationData")
            .finish_non_exhaustive()
    }
}

impl RuntimeCancellationData {
    /// Creates a runtime cancellation data router.
    #[must_use]
    pub fn new(dependency: Arc<dyn RuntimeCancellationDependencyPort>) -> Self {
        Self { dependency }
    }
}

impl RuntimeCancellationDataPort for RuntimeCancellationData {
    fn cancellation_requested(
        &self,
        request: RuntimeCancellationDataRequest,
    ) -> Result<bool, RuntimeCancellationDataError> {
        self.dependency
            .cancellation_requested(&request.cancellation_id)
            .map_err(map_dependency_error)
    }
}

impl RuntimeCancellationControlDataPort for RuntimeCancellationData {
    fn request_runtime_cancellation(
        &self,
        command: RequestRuntimeCancellationDataCommand,
    ) -> Result<(), RuntimeCancellationDataError> {
        self.dependency
            .request_cancellation(&command.cancellation_id)
            .map_err(map_dependency_error)
    }

    fn clear_runtime_cancellation(
        &self,
        command: ClearRuntimeCancellationDataCommand,
    ) -> Result<bool, RuntimeCancellationDataError> {
        self.dependency
            .clear_cancellation(&command.cancellation_id)
            .map_err(map_dependency_error)
    }
}

fn map_dependency_error(error: RuntimeCancellationDependencyError) -> RuntimeCancellationDataError {
    match error {
        RuntimeCancellationDependencyError::InvalidRequest => RuntimeCancellationDataError::Invalid,
        RuntimeCancellationDependencyError::Capacity
        | RuntimeCancellationDependencyError::Unavailable => {
            RuntimeCancellationDataError::Unavailable
        }
    }
}

/// Stable cancellation data failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RuntimeCancellationDataError {
    /// Cancellation identity was invalid.
    #[error("runtime cancellation data request is invalid")]
    Invalid,
    /// Cancellation state was unavailable.
    #[error("runtime cancellation data is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use agentmod_runtime_dependency::cancellation::RuntimeCancellationDependency;

    use super::*;

    #[test]
    fn control_and_query_share_state_across_data_clones() {
        let data = RuntimeCancellationData::new(Arc::new(RuntimeCancellationDependency::default()));
        let query = data.clone();
        data.request_runtime_cancellation(RequestRuntimeCancellationDataCommand {
            cancellation_id: String::from("plugin-turn:shared"),
        })
        .expect("request");
        assert!(
            query
                .cancellation_requested(RuntimeCancellationDataRequest {
                    cancellation_id: String::from("plugin-turn:shared"),
                })
                .expect("query")
        );
        assert!(
            query
                .clear_runtime_cancellation(ClearRuntimeCancellationDataCommand {
                    cancellation_id: String::from("plugin-turn:shared"),
                })
                .expect("clear")
        );
        assert!(
            !data
                .cancellation_requested(RuntimeCancellationDataRequest {
                    cancellation_id: String::from("plugin-turn:shared"),
                })
                .expect("query")
        );
    }
}
