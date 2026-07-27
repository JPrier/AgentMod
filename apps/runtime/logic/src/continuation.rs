//! Runtime business semantics for durable continuation creation and resolution.

use agentmod_primitives::{ContinuationId, TimestampMillis};
use agentmod_runtime_data::continuation::{
    ContinuationDataError, ContinuationDataPort, ContinuationPayloadRecord, ContinuationRecord,
    ContinuationStateRecord, ContinuationWakeRecord, CreateContinuationDataRequest,
    PendingToolCallPayloadRecord, ResolveContinuationDataRequest, ToolApprovalPayloadRecord,
};
use serde_json::Value;
use thiserror::Error;

/// Logic-owned wake condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuationWakeCondition {
    /// Explicit user or supervisor resolution.
    Manual,
    /// Eligible at a wall-clock threshold.
    At(TimestampMillis),
    /// Eligible after a matching committed event.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
        /// Optional constrained selector.
        selector: Option<String>,
    },
    /// Eligible after matching supervised process output.
    ProcessOutput {
        /// Runtime process identifier.
        process_id: String,
        /// Literal or constrained pattern.
        pattern: String,
    },
}

/// Business command to create a durable continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateContinuationCommand {
    /// Session containing this durable continuation.
    pub session_id: String,
    /// Opaque identifier produced by the runtime's ID source.
    pub id: ContinuationId,
    /// Wake semantics.
    pub wake_condition: ContinuationWakeCondition,
    /// Pending action reconstructed after a decision.
    pub payload: ContinuationPayload,
    /// Optional expiration.
    pub expires_at: Option<TimestampMillis>,
}

/// Business command to resolve an approval continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveApprovalCommand {
    /// Session containing the continuation.
    pub session_id: String,
    /// Durable continuation.
    pub id: ContinuationId,
    /// Approval or denial.
    pub approved: bool,
}

/// Business result of an idempotent approval resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveApprovalResult {
    /// True only for the request that won the durable transition.
    pub transitioned: bool,
    /// Endpoint-safe terminal disposition.
    pub disposition: ApprovalDisposition,
    /// Pending action associated with this decision.
    pub payload: ContinuationPayload,
}

/// Business query for one durable continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadContinuationQuery {
    /// Session containing the continuation.
    pub session_id: String,
    /// Durable continuation identifier.
    pub id: ContinuationId,
}

/// Logic-owned durable continuation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationState {
    /// Awaiting resolution.
    Pending,
    /// Approved and claimed.
    Resumed,
    /// Denied.
    Cancelled,
    /// Expired.
    Expired,
}

/// Business result for one durable continuation query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadContinuationResult {
    /// Current durable state.
    pub state: ContinuationState,
    /// Pending action associated with the continuation.
    pub payload: ContinuationPayload,
}

/// Logic-owned durable pending action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuationPayload {
    /// Final intercepted tool call plus the turn state needed to continue.
    ToolApproval(Box<ToolApprovalContinuation>),
    /// Storage-only marker for callers without an executable action.
    Opaque(String),
}

/// Logic-owned restart-safe tool approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolApprovalContinuation {
    /// Canonical session containing the pending action.
    pub session_id: String,
    /// Workspace selected for the turn.
    pub workspace: String,
    /// Provider-issued tool-call identifier.
    pub call_id: String,
    /// Stable internal tool name.
    pub tool: String,
    /// Intercepted and policy-checked tool arguments.
    pub arguments: Value,
    /// Cancellation identifier associated with the turn.
    pub cancellation_id: String,
    /// Provider adapter selected for the turn.
    pub provider: String,
    /// Provider model selected for the turn.
    pub model: String,
    /// Provider-specific request options.
    pub options: Value,
    /// Session style selected for execution.
    pub style: String,
    /// Harness continuation that originally emitted the tool call.
    pub harness_continuation: String,
    /// Remaining sibling calls in the same provider batch.
    pub remaining_tool_calls: Vec<PendingToolCallContinuation>,
}

/// Logic-owned sibling tool call retained behind an approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingToolCallContinuation {
    /// Harness continuation alias for the batch.
    pub harness_continuation: String,
    /// Provider call identifier.
    pub call_id: String,
    /// Stable internal tool name.
    pub tool: String,
    /// Provider-supplied arguments.
    pub arguments: Value,
}

/// Approval disposition after resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDisposition {
    /// Approved action is claimed for continuation.
    Approved,
    /// Action is durably denied.
    Denied,
}

/// Narrow continuation use-case interface consumed by runtime service.
pub trait ContinuationLogicPort {
    /// Creates a durable pending continuation before requesting approval.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] when business validation or persistence fails.
    fn create_continuation(
        &self,
        command: CreateContinuationCommand,
    ) -> Result<(), ContinuationLogicError>;

    /// Atomically resolves an approval.
    ///
    /// Exactly one caller receives `transitioned == true`; duplicate equal
    /// decisions are idempotent, while conflicting decisions are errors.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] for state conflicts or persistence failure.
    fn resolve_approval(
        &self,
        command: ResolveApprovalCommand,
    ) -> Result<ResolveApprovalResult, ContinuationLogicError>;

    /// Loads one continuation without changing its state.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] for invalid scope or persistence failure.
    fn load_continuation(
        &self,
        query: LoadContinuationQuery,
    ) -> Result<LoadContinuationResult, ContinuationLogicError>;
}

/// Continuation business implementation over data only.
#[derive(Clone, Debug)]
pub struct ContinuationLogic<D> {
    data: D,
}

impl<D> ContinuationLogic<D> {
    /// Creates continuation logic.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D> ContinuationLogicPort for ContinuationLogic<D>
where
    D: ContinuationDataPort,
{
    fn create_continuation(
        &self,
        command: CreateContinuationCommand,
    ) -> Result<(), ContinuationLogicError> {
        if command.session_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        validate_wake_condition(&command.wake_condition)?;
        validate_payload(&command.session_id, &command.payload)?;
        self.data
            .create(CreateContinuationDataRequest {
                record: ContinuationRecord {
                    session_id: command.session_id,
                    id: command.id.to_string(),
                    state: ContinuationStateRecord::Pending,
                    wake_condition: to_data_wake(command.wake_condition),
                    payload: to_data_payload(command.payload),
                    expires_at_millis: command.expires_at.map(TimestampMillis::get),
                },
            })
            .map_err(ContinuationLogicError::Data)
    }

    fn resolve_approval(
        &self,
        command: ResolveApprovalCommand,
    ) -> Result<ResolveApprovalResult, ContinuationLogicError> {
        let record = self
            .data
            .resolve(ResolveContinuationDataRequest {
                session_id: command.session_id,
                id: command.id.to_string(),
                approved: command.approved,
            })
            .map_err(ContinuationLogicError::Data)?;
        let disposition = match record.state {
            ContinuationStateRecord::Resumed => ApprovalDisposition::Approved,
            ContinuationStateRecord::Cancelled => ApprovalDisposition::Denied,
            ContinuationStateRecord::Pending | ContinuationStateRecord::Expired => {
                return Err(ContinuationLogicError::InvalidResolutionState);
            }
        };
        Ok(ResolveApprovalResult {
            transitioned: record.transitioned,
            disposition,
            payload: from_data_payload(record.payload),
        })
    }

    fn load_continuation(
        &self,
        query: LoadContinuationQuery,
    ) -> Result<LoadContinuationResult, ContinuationLogicError> {
        if query.session_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        let record = self
            .data
            .load(&query.session_id, &query.id.to_string())
            .map_err(ContinuationLogicError::Data)?;
        let payload = from_data_payload(record.payload);
        validate_payload(&query.session_id, &payload)?;
        Ok(LoadContinuationResult {
            state: match record.state {
                ContinuationStateRecord::Pending => ContinuationState::Pending,
                ContinuationStateRecord::Resumed => ContinuationState::Resumed,
                ContinuationStateRecord::Cancelled => ContinuationState::Cancelled,
                ContinuationStateRecord::Expired => ContinuationState::Expired,
            },
            payload,
        })
    }
}

fn validate_wake_condition(
    wake_condition: &ContinuationWakeCondition,
) -> Result<(), ContinuationLogicError> {
    match wake_condition {
        ContinuationWakeCondition::RuntimeEvent { event_type, .. }
            if event_type.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidWakeCondition)
        }
        ContinuationWakeCondition::ProcessOutput {
            process_id,
            pattern,
        } if process_id.trim().is_empty() || pattern.is_empty() => {
            Err(ContinuationLogicError::InvalidWakeCondition)
        }
        _ => Ok(()),
    }
}

fn to_data_wake(wake_condition: ContinuationWakeCondition) -> ContinuationWakeRecord {
    match wake_condition {
        ContinuationWakeCondition::Manual => ContinuationWakeRecord::Manual,
        ContinuationWakeCondition::At(timestamp) => ContinuationWakeRecord::At(timestamp.get()),
        ContinuationWakeCondition::RuntimeEvent {
            event_type,
            selector,
        } => ContinuationWakeRecord::RuntimeEvent {
            event_type,
            selector,
        },
        ContinuationWakeCondition::ProcessOutput {
            process_id,
            pattern,
        } => ContinuationWakeRecord::ProcessOutput {
            process_id,
            pattern,
        },
    }
}

fn validate_payload(
    session_id: &str,
    payload: &ContinuationPayload,
) -> Result<(), ContinuationLogicError> {
    match payload {
        ContinuationPayload::ToolApproval(tool)
            if tool.session_id != session_id
                || tool.workspace.trim().is_empty()
                || tool.call_id.trim().is_empty()
                || tool.tool.trim().is_empty()
                || !tool.arguments.is_object()
                || tool.cancellation_id.trim().is_empty()
                || tool.provider.trim().is_empty()
                || tool.model.trim().is_empty()
                || !tool.options.is_object()
                || tool.style.trim().is_empty()
                || tool.harness_continuation.trim().is_empty()
                || tool.remaining_tool_calls.len() > 64
                || tool.remaining_tool_calls.iter().any(|pending| {
                    pending.harness_continuation.trim().is_empty()
                        || pending.call_id.trim().is_empty()
                        || pending.tool.trim().is_empty()
                        || !pending.arguments.is_object()
                }) =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::Opaque(label) if label.trim().is_empty() => {
            Err(ContinuationLogicError::InvalidPayload)
        }
        _ => Ok(()),
    }
}

fn to_data_payload(payload: ContinuationPayload) -> ContinuationPayloadRecord {
    match payload {
        ContinuationPayload::ToolApproval(tool) => {
            ContinuationPayloadRecord::ToolApproval(Box::new(ToolApprovalPayloadRecord {
                session_id: tool.session_id,
                workspace: tool.workspace,
                call_id: tool.call_id,
                tool: tool.tool,
                arguments: tool.arguments,
                cancellation_id: tool.cancellation_id,
                provider: tool.provider,
                model: tool.model,
                options: tool.options,
                style: tool.style,
                harness_continuation: tool.harness_continuation,
                remaining_tool_calls: tool
                    .remaining_tool_calls
                    .into_iter()
                    .map(|pending| PendingToolCallPayloadRecord {
                        harness_continuation: pending.harness_continuation,
                        call_id: pending.call_id,
                        tool: pending.tool,
                        arguments: pending.arguments,
                    })
                    .collect(),
            }))
        }
        ContinuationPayload::Opaque(label) => ContinuationPayloadRecord::Opaque { label },
    }
}

fn from_data_payload(payload: ContinuationPayloadRecord) -> ContinuationPayload {
    match payload {
        ContinuationPayloadRecord::ToolApproval(tool) => {
            ContinuationPayload::ToolApproval(Box::new(ToolApprovalContinuation {
                session_id: tool.session_id,
                workspace: tool.workspace,
                call_id: tool.call_id,
                tool: tool.tool,
                arguments: tool.arguments,
                cancellation_id: tool.cancellation_id,
                provider: tool.provider,
                model: tool.model,
                options: tool.options,
                style: tool.style,
                harness_continuation: tool.harness_continuation,
                remaining_tool_calls: tool
                    .remaining_tool_calls
                    .into_iter()
                    .map(|pending| PendingToolCallContinuation {
                        harness_continuation: pending.harness_continuation,
                        call_id: pending.call_id,
                        tool: pending.tool,
                        arguments: pending.arguments,
                    })
                    .collect(),
            }))
        }
        ContinuationPayloadRecord::Opaque { label } => ContinuationPayload::Opaque(label),
    }
}

/// Continuation business failure.
#[derive(Debug, Error)]
pub enum ContinuationLogicError {
    /// Session scope is missing.
    #[error("continuation session is invalid")]
    InvalidSession,
    /// Wake condition cannot be evaluated safely.
    #[error("continuation wake condition is invalid")]
    InvalidWakeCondition,
    /// Pending action cannot be reconstructed safely.
    #[error("continuation payload is invalid")]
    InvalidPayload,
    /// Data returned a state inconsistent with a completed resolution.
    #[error("continuation resolution returned an invalid state")]
    InvalidResolutionState,
    /// Continuation dataset failed.
    #[error("continuation data failed: {0}")]
    Data(#[source] ContinuationDataError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use uuid::Uuid;

    use agentmod_runtime_data::continuation::ResolveContinuationDataRecord;

    use super::*;

    struct MockData {
        creates: RefCell<Vec<CreateContinuationDataRequest>>,
        resolutions: RefCell<Vec<ResolveContinuationDataRequest>>,
        state: ContinuationStateRecord,
        transitioned: bool,
    }

    impl ContinuationDataPort for MockData {
        fn create(
            &self,
            request: CreateContinuationDataRequest,
        ) -> Result<(), ContinuationDataError> {
            self.creates.borrow_mut().push(request);
            Ok(())
        }

        fn load(
            &self,
            _session_id: &str,
            _id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            unreachable!("not used by this logic use case")
        }

        fn resolve(
            &self,
            request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            self.resolutions.borrow_mut().push(request);
            Ok(ResolveContinuationDataRecord {
                transitioned: self.transitioned,
                state: self.state,
                payload: ContinuationPayloadRecord::Opaque {
                    label: "fixture".into(),
                },
            })
        }
    }

    fn id() -> ContinuationId {
        ContinuationId::from_uuid(Uuid::from_u128(1))
    }

    #[test]
    fn creates_pending_data_record() {
        let logic = ContinuationLogic::new(MockData {
            creates: RefCell::new(Vec::new()),
            resolutions: RefCell::new(Vec::new()),
            state: ContinuationStateRecord::Pending,
            transitioned: false,
        });
        logic
            .create_continuation(CreateContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                wake_condition: ContinuationWakeCondition::Manual,
                payload: ContinuationPayload::Opaque("fixture".into()),
                expires_at: None,
            })
            .expect("create");
        let observed = logic.data.creates.into_inner();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].record.state, ContinuationStateRecord::Pending);
        assert_eq!(observed[0].record.id, id().to_string());
    }

    #[test]
    fn returns_transition_winner_for_approved_resolution() {
        let logic = ContinuationLogic::new(MockData {
            creates: RefCell::new(Vec::new()),
            resolutions: RefCell::new(Vec::new()),
            state: ContinuationStateRecord::Resumed,
            transitioned: true,
        });
        assert_eq!(
            logic
                .resolve_approval(ResolveApprovalCommand {
                    session_id: "session_1".into(),
                    id: id(),
                    approved: true,
                })
                .expect("resolve"),
            ResolveApprovalResult {
                transitioned: true,
                disposition: ApprovalDisposition::Approved,
                payload: ContinuationPayload::Opaque("fixture".into()),
            }
        );
        assert_eq!(
            logic.data.resolutions.into_inner(),
            vec![ResolveContinuationDataRequest {
                session_id: "session_1".into(),
                id: id().to_string(),
                approved: true,
            }]
        );
    }

    #[test]
    fn rejects_unsafe_empty_wake_selectors_before_data() {
        let logic = ContinuationLogic::new(MockData {
            creates: RefCell::new(Vec::new()),
            resolutions: RefCell::new(Vec::new()),
            state: ContinuationStateRecord::Pending,
            transitioned: false,
        });
        assert!(matches!(
            logic.create_continuation(CreateContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                wake_condition: ContinuationWakeCondition::ProcessOutput {
                    process_id: String::new(),
                    pattern: "ready".into(),
                },
                payload: ContinuationPayload::Opaque("fixture".into()),
                expires_at: None,
            }),
            Err(ContinuationLogicError::InvalidWakeCondition)
        ));
        assert!(logic.data.creates.borrow().is_empty());
    }
}
