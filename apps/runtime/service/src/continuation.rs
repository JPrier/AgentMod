//! Endpoint mapping for durable approval continuations.

use agentmod_runtime_logic::continuation::{
    ApprovalDisposition, ContinuationLogicError, ContinuationLogicPort, ResolveApprovalCommand,
};
use agentmod_runtime_protocol::{RuntimeRequest, RuntimeResponse};
use thiserror::Error;

/// Service-owned approval request after wire parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceResolveApprovalRequest {
    /// Session containing the approval.
    pub session_id: String,
    /// Opaque wire text, parsed before entering logic.
    pub continuation_id: String,
    /// Endpoint approval choice.
    pub approved: bool,
}

/// Service-owned approval result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceResolveApprovalResponse {
    /// True only for the winning durable transition.
    pub transitioned: bool,
    /// Endpoint-safe outcome.
    pub outcome: ServiceApprovalOutcome,
}

/// Endpoint-safe approval outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceApprovalOutcome {
    /// Action may resume.
    Approved,
    /// Action is denied.
    Denied,
}

/// Narrow endpoint service for continuation operations.
#[derive(Clone, Debug)]
pub struct ContinuationService<L> {
    logic: L,
}

impl<L> ContinuationService<L> {
    /// Creates a continuation service over injected logic.
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> ContinuationService<L>
where
    L: ContinuationLogicPort,
{
    /// Handles continuation-related runtime wire requests.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationServiceError`] for unsupported or invalid requests
    /// and translated business failures.
    pub fn handle_wire(
        &self,
        request: &RuntimeRequest,
    ) -> Result<RuntimeResponse, ContinuationServiceError> {
        let RuntimeRequest::ResolveApproval {
            session_id,
            continuation_id,
            approved,
        } = request
        else {
            return Err(ContinuationServiceError::UnsupportedEndpoint);
        };
        let response = self.resolve_approval(&ServiceResolveApprovalRequest {
            session_id: session_id.to_string(),
            continuation_id: continuation_id.clone(),
            approved: *approved,
        })?;
        Ok(RuntimeResponse::ApprovalResolved {
            transitioned: response.transitioned,
            events: Vec::new(),
            last_committed_sequence: None,
            awaiting_continuation: None,
        })
    }

    /// Parses and executes one service-owned approval request.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationServiceError::InvalidContinuationId`] for invalid
    /// wire text or [`ContinuationServiceError::Logic`] for a business failure.
    pub fn resolve_approval(
        &self,
        request: &ServiceResolveApprovalRequest,
    ) -> Result<ServiceResolveApprovalResponse, ContinuationServiceError> {
        let id = request
            .continuation_id
            .parse()
            .map_err(|_| ContinuationServiceError::InvalidContinuationId)?;
        let result = self
            .logic
            .resolve_approval(ResolveApprovalCommand {
                session_id: request.session_id.clone(),
                id,
                approved: request.approved,
            })
            .map_err(ContinuationServiceError::Logic)?;
        Ok(ServiceResolveApprovalResponse {
            transitioned: result.transitioned,
            outcome: match result.disposition {
                ApprovalDisposition::Approved => ServiceApprovalOutcome::Approved,
                ApprovalDisposition::Denied => ServiceApprovalOutcome::Denied,
            },
        })
    }
}

/// Approval endpoint failure.
#[derive(Debug, Error)]
pub enum ContinuationServiceError {
    /// Request belongs to another runtime endpoint.
    #[error("runtime endpoint is not a continuation endpoint")]
    UnsupportedEndpoint,
    /// Wire token is not a valid portable continuation ID.
    #[error("continuation identifier is invalid")]
    InvalidContinuationId,
    /// Business use case failed.
    #[error("continuation operation failed: {0}")]
    Logic(#[source] ContinuationLogicError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_logic::continuation::{ContinuationPayload, ResolveApprovalResult};

    use super::*;

    struct MockLogic {
        observed: RefCell<Vec<ResolveApprovalCommand>>,
        disposition: ApprovalDisposition,
        transitioned: bool,
    }

    impl ContinuationLogicPort for MockLogic {
        fn create_continuation(
            &self,
            _command: agentmod_runtime_logic::continuation::CreateContinuationCommand,
        ) -> Result<(), ContinuationLogicError> {
            unreachable!("creation is not exposed by this frontend endpoint")
        }

        fn resolve_approval(
            &self,
            command: ResolveApprovalCommand,
        ) -> Result<ResolveApprovalResult, ContinuationLogicError> {
            self.observed.borrow_mut().push(command);
            Ok(ResolveApprovalResult {
                transitioned: self.transitioned,
                disposition: self.disposition,
                payload: ContinuationPayload::Opaque("fixture".into()),
            })
        }

        fn load_continuation(
            &self,
            _query: agentmod_runtime_logic::continuation::LoadContinuationQuery,
        ) -> Result<
            agentmod_runtime_logic::continuation::LoadContinuationResult,
            ContinuationLogicError,
        > {
            unreachable!("loading is not exposed by this storage-only endpoint")
        }
    }

    fn service() -> ContinuationService<MockLogic> {
        ContinuationService::new(MockLogic {
            observed: RefCell::new(Vec::new()),
            disposition: ApprovalDisposition::Approved,
            transitioned: true,
        })
    }

    #[test]
    fn maps_wire_request_through_service_owned_request() {
        let id = "00000000-0000-0000-0000-000000000001";
        let service = service();
        assert_eq!(
            service
                .handle_wire(&RuntimeRequest::ResolveApproval {
                    session_id: "00000000-0000-0000-0000-000000000002"
                        .parse()
                        .expect("session"),
                    continuation_id: id.into(),
                    approved: true,
                })
                .expect("approval"),
            RuntimeResponse::ApprovalResolved {
                transitioned: true,
                events: Vec::new(),
                last_committed_sequence: None,
                awaiting_continuation: None,
            }
        );
        let observed = service.logic.observed.into_inner();
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].session_id,
            "00000000-0000-0000-0000-000000000002"
        );
        assert_eq!(observed[0].id.to_string(), id);
        assert!(observed[0].approved);
    }

    #[test]
    fn rejects_invalid_wire_id_before_logic() {
        let service = service();
        assert!(matches!(
            service.resolve_approval(&ServiceResolveApprovalRequest {
                session_id: "00000000-0000-0000-0000-000000000002".into(),
                continuation_id: "../bad".into(),
                approved: true,
            }),
            Err(ContinuationServiceError::InvalidContinuationId)
        ));
        assert!(service.logic.observed.borrow().is_empty());
    }
}
