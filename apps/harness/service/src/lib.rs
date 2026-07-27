//! Endpoint mapping for the native harness.

pub mod execution;

use agentmod_harness_logic::{
    HarnessHealthLogic, HarnessHealthResult, HarnessHealthStatus, InspectHarnessHealthCommand,
    LogicError,
};
use agentmod_harness_protocol::HarnessCommand;
use thiserror::Error;

/// Service-owned health endpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHealthRequest {
    /// Capabilities whose readiness should be reported.
    pub required_capabilities: Vec<String>,
}

/// Service-owned health status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceHealthStatus {
    /// All requested capability checks passed.
    Ok,
    /// The harness can accept work with reduced capability.
    Degraded,
    /// The harness cannot accept provider work.
    Unavailable,
}

impl ServiceHealthStatus {
    /// Stable transport-safe status name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Service-owned health endpoint response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHealthResponse {
    /// Endpoint health status.
    pub status: ServiceHealthStatus,
    /// Harness package version.
    pub version: String,
    /// Providers configured in the harness catalog.
    pub configured_provider_count: u32,
    /// Providers currently ready.
    pub ready_provider_count: u32,
    /// Stable ordered capability names.
    pub capabilities: Vec<String>,
    /// Stable ordered missing capability names.
    pub missing_capabilities: Vec<String>,
}

/// Service-owned endpoint response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceResponse {
    /// Harness health response.
    Health(ServiceHealthResponse),
}

/// Endpoint-facing harness service failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ServiceError {
    /// This vertical slice does not expose the received command.
    #[error("harness command `{command}` is not available at this endpoint")]
    UnsupportedCommand {
        /// Stable command kind without command contents.
        command: &'static str,
    },
    /// Business logic rejected or could not complete the request.
    #[error("harness health request failed: {message}")]
    HealthFailed {
        /// Redacted service-safe diagnostic.
        message: String,
    },
}

/// Endpoint-facing harness service.
#[derive(Clone, Debug)]
pub struct HarnessService<L> {
    logic: L,
}

impl<L> HarnessService<L> {
    /// Injects the harness logic implementation.
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> HarnessService<L>
where
    L: HarnessHealthLogic,
{
    /// Maps one harness wire command and invokes only the logic layer.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for unsupported commands or health-evaluation failures.
    pub fn handle_wire_command(
        &self,
        command: &HarnessCommand,
    ) -> Result<ServiceResponse, ServiceError> {
        let request = to_service_request(command)?;
        self.health(request).map(ServiceResponse::Health)
    }

    /// Executes the health endpoint using service-owned types.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when harness logic cannot evaluate health.
    pub fn health(
        &self,
        request: ServiceHealthRequest,
    ) -> Result<ServiceHealthResponse, ServiceError> {
        let command = to_logic_command(request);
        let result = self
            .logic
            .inspect_health(command)
            .map_err(map_logic_error)?;
        Ok(to_service_response(result))
    }
}

fn to_service_request(command: &HarnessCommand) -> Result<ServiceHealthRequest, ServiceError> {
    match command {
        HarnessCommand::Health => Ok(ServiceHealthRequest {
            required_capabilities: Vec::new(),
        }),
        HarnessCommand::Execute { .. } => {
            Err(ServiceError::UnsupportedCommand { command: "execute" })
        }
        HarnessCommand::Continue { .. } => Err(ServiceError::UnsupportedCommand {
            command: "continue",
        }),
        HarnessCommand::Cancel { .. } => {
            Err(ServiceError::UnsupportedCommand { command: "cancel" })
        }
    }
}

fn to_logic_command(request: ServiceHealthRequest) -> InspectHarnessHealthCommand {
    InspectHarnessHealthCommand {
        required_capabilities: request.required_capabilities,
    }
}

fn to_service_response(result: HarnessHealthResult) -> ServiceHealthResponse {
    let status = match result.status {
        HarnessHealthStatus::Ready => ServiceHealthStatus::Ok,
        HarnessHealthStatus::Degraded => ServiceHealthStatus::Degraded,
        HarnessHealthStatus::Unavailable => ServiceHealthStatus::Unavailable,
    };
    ServiceHealthResponse {
        status,
        version: env!("CARGO_PKG_VERSION").to_owned(),
        configured_provider_count: result.configured_provider_count,
        ready_provider_count: result.ready_provider_count,
        capabilities: result.available_capabilities,
        missing_capabilities: result.missing_capabilities,
    }
}

fn map_logic_error(error: LogicError) -> ServiceError {
    let message = match error {
        LogicError::InvalidCapability { capability } => {
            format!("invalid required capability `{capability}`")
        }
        LogicError::HealthDataUnavailable { detail } => {
            format!("harness health data is unavailable: {detail}")
        }
    };
    ServiceError::HealthFailed { message }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockHealthLogic {
        commands: Mutex<Vec<InspectHarnessHealthCommand>>,
        response: Result<HarnessHealthResult, LogicError>,
    }

    impl HarnessHealthLogic for MockHealthLogic {
        fn inspect_health(
            &self,
            command: InspectHarnessHealthCommand,
        ) -> Result<HarnessHealthResult, LogicError> {
            self.commands
                .lock()
                .expect("command lock is not poisoned")
                .push(command);
            self.response.clone()
        }
    }

    #[test]
    fn maps_wire_health_through_logic_and_back_to_service() {
        let logic = MockHealthLogic {
            commands: Mutex::new(Vec::new()),
            response: Ok(HarnessHealthResult {
                status: HarnessHealthStatus::Ready,
                configured_provider_count: 1,
                ready_provider_count: 1,
                available_capabilities: vec!["streaming".to_owned()],
                missing_capabilities: Vec::new(),
            }),
        };
        let service = HarnessService::new(logic);

        let response = service
            .handle_wire_command(&HarnessCommand::Health)
            .expect("mock logic succeeds");

        assert_eq!(
            response,
            ServiceResponse::Health(ServiceHealthResponse {
                status: ServiceHealthStatus::Ok,
                version: env!("CARGO_PKG_VERSION").to_owned(),
                configured_provider_count: 1,
                ready_provider_count: 1,
                capabilities: vec!["streaming".to_owned()],
                missing_capabilities: Vec::new(),
            })
        );
        assert_eq!(
            service
                .logic
                .commands
                .lock()
                .expect("command lock is not poisoned")
                .as_slice(),
            &[InspectHarnessHealthCommand {
                required_capabilities: Vec::new()
            }]
        );
    }

    #[test]
    fn maps_logic_error_without_leaking_logic_error_type() {
        let service = HarnessService::new(MockHealthLogic {
            commands: Mutex::new(Vec::new()),
            response: Err(LogicError::HealthDataUnavailable {
                detail: "fixture".to_owned(),
            }),
        });

        assert_eq!(
            service.health(ServiceHealthRequest {
                required_capabilities: Vec::new(),
            }),
            Err(ServiceError::HealthFailed {
                message: "harness health data is unavailable: fixture".to_owned(),
            })
        );
    }
}
