//! Business-facing durable continuation datasets.

use agentmod_runtime_dependency::continuation::{
    ContinuationDependencyError, ContinuationDependencyPort, DependencyContinuationRecord,
    DependencyContinuationState, DependencyCreateContinuationRequest,
    DependencyTransitionContinuationRequest,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Data-owned wake condition record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContinuationWakeRecord {
    /// Explicit resolution.
    Manual,
    /// Time threshold in Unix milliseconds.
    At(i64),
    /// Matching committed runtime event.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
        /// Optional constrained selector.
        selector: Option<String>,
    },
    /// Matching output from a supervised process.
    ProcessOutput {
        /// Runtime process identifier.
        process_id: String,
        /// Literal or constrained pattern.
        pattern: String,
    },
}

/// Data-owned continuation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationStateRecord {
    /// Awaiting resolution.
    Pending,
    /// Approved and claimed.
    Resumed,
    /// Denied.
    Cancelled,
    /// Expired.
    Expired,
}

/// Data-owned pending action reconstructed only after a durable decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContinuationPayloadRecord {
    /// An intercepted provider tool call waiting for user approval.
    ToolApproval(Box<ToolApprovalPayloadRecord>),
    /// A compiled style `user_approval` node waiting for resolution.
    StyleApproval(Box<StyleApprovalPayloadRecord>),
    /// A complete provider turn deferred until an authenticated scheduler claim.
    DeferredTurn(Box<DeferredTurnPayloadRecord>),
    /// A durable runtime-owned child-creation approval waiting for resolution.
    ChildApproval(Box<ChildApprovalPayloadRecord>),
    /// Generic fixture payload used by storage-only callers.
    Opaque {
        /// Stable non-secret label.
        label: String,
    },
}

/// Data-owned restart-safe compiled-style approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleApprovalPayloadRecord {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// User-authored input owning the graph execution.
    pub prompt: String,
    /// Provider retained for exact command identity.
    pub provider: String,
    /// Model retained for exact command identity.
    pub model: String,
    /// Style-specific and provider options.
    pub options: serde_json::Value,
    /// Explicit session style.
    pub style: String,
    /// Stable cancellation identity for the graph execution.
    pub cancellation_id: String,
    /// Exact compiled-style cache key selected by the session.
    pub compiled_style_cache_key: String,
    /// Active graph node requesting the decision.
    pub node_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Canonical hash of caller-controlled graph inputs.
    pub request_reference: String,
}

/// Data-owned restart-safe deferred provider turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeferredTurnPayloadRecord {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Schedule allowed to claim this continuation.
    pub schedule_id: String,
    /// User-authored prompt.
    pub prompt: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// Provider selected for the resumed turn.
    pub provider: String,
    /// Model selected for the resumed turn.
    pub model: String,
    /// Provider-specific options.
    pub options: serde_json::Value,
    /// Explicit session style.
    pub style: String,
    /// Stable cancellation identity for the deferred turn.
    pub cancellation_id: String,
}

/// Data-owned restart-safe durable child-creation approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildApprovalPayloadRecord {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// Provider selected for the resumed turn.
    pub provider: String,
    /// Model selected for the resumed turn.
    pub model: String,
    /// Provider-specific options.
    pub options: serde_json::Value,
    /// Explicit session style.
    pub style: String,
    /// Stable cancellation identity for the turn.
    pub cancellation_id: String,
    /// Exact child execution identity.
    pub execution_id: String,
    /// Runtime-owned task identity.
    pub task_id: String,
    /// Spawn node that owns the child.
    pub node_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Exact bound child style selector.
    pub child_style: String,
    /// Exact bound workspace mode.
    pub workspace_mode: String,
    /// Exact bound tool groups.
    pub tool_groups: Vec<String>,
    /// Exact bound token budget.
    pub token_budget: u64,
    /// Portable approval expiry in Unix milliseconds.
    pub expires_at_ms: i64,
}

/// Data-owned restart-safe tool approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolApprovalPayloadRecord {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// Provider call identifier.
    pub call_id: String,
    /// Final intercepted tool identifier.
    pub tool: String,
    /// Final intercepted arguments.
    pub arguments: serde_json::Value,
    /// Turn cancellation identifier.
    pub cancellation_id: String,
    /// Provider selected for the resumed projection.
    pub provider: String,
    /// Model selected for the resumed projection.
    pub model: String,
    /// Provider-specific options.
    pub options: serde_json::Value,
    /// Explicit session style.
    pub style: String,
    /// Original in-memory harness continuation, usable before restart.
    pub harness_continuation: String,
    /// Sibling calls that must finish before the provider batch may resume.
    #[serde(default)]
    pub remaining_tool_calls: Vec<PendingToolCallPayloadRecord>,
}

/// Data-owned sibling tool call retained behind an approval.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingToolCallPayloadRecord {
    /// Harness continuation alias for the batch.
    pub harness_continuation: String,
    /// Provider call identifier.
    pub call_id: String,
    /// Stable internal tool name.
    pub tool: String,
    /// Provider-supplied arguments.
    pub arguments: serde_json::Value,
}

/// Data-owned continuation record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuationRecord {
    /// Session containing the continuation.
    pub session_id: String,
    /// Opaque continuation identifier.
    pub id: String,
    /// Durable state.
    pub state: ContinuationStateRecord,
    /// Durable wake condition.
    pub wake_condition: ContinuationWakeRecord,
    /// Pending action required for restart-safe resumption.
    pub payload: ContinuationPayloadRecord,
    /// Optional expiry in Unix milliseconds.
    pub expires_at_millis: Option<i64>,
}

/// Data request to persist a new continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateContinuationDataRequest {
    /// Initial pending record.
    pub record: ContinuationRecord,
}

/// Data request to resolve an approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveContinuationDataRequest {
    /// Session containing the continuation.
    pub session_id: String,
    /// Continuation identifier.
    pub id: String,
    /// Desired terminal state.
    pub approved: bool,
}

/// Data result for an idempotent resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveContinuationDataRecord {
    /// True only for the winning transition.
    pub transitioned: bool,
    /// Durable state after resolution.
    pub state: ContinuationStateRecord,
    /// Durable pending action payload.
    pub payload: ContinuationPayloadRecord,
}

/// Narrow data interface consumed by continuation logic.
pub trait ContinuationDataPort {
    /// Persists a new pending continuation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for invalid records or adapter failures.
    fn create(&self, request: CreateContinuationDataRequest) -> Result<(), ContinuationDataError>;

    /// Loads one durable continuation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for missing, corrupt, or inaccessible records.
    fn load(&self, session_id: &str, id: &str)
    -> Result<ContinuationRecord, ContinuationDataError>;

    /// Resolves a pending continuation using an atomic dependency operation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for state conflicts or adapter failures.
    fn resolve(
        &self,
        request: ResolveContinuationDataRequest,
    ) -> Result<ResolveContinuationDataRecord, ContinuationDataError>;
}

/// Data router for continuation persistence.
#[derive(Clone, Debug)]
pub struct ContinuationData<D> {
    dependency: D,
}

impl<D> ContinuationData<D> {
    /// Creates a router over one injected dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D> ContinuationDataPort for ContinuationData<D>
where
    D: ContinuationDependencyPort,
{
    fn create(&self, request: CreateContinuationDataRequest) -> Result<(), ContinuationDataError> {
        if request.record.state != ContinuationStateRecord::Pending {
            return Err(ContinuationDataError::InvalidInitialState);
        }
        let dependency_record = to_dependency_record(request.record)?;
        self.dependency
            .create_continuation(DependencyCreateContinuationRequest {
                record: dependency_record,
            })
            .map_err(ContinuationDataError::Dependency)
    }

    fn load(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<ContinuationRecord, ContinuationDataError> {
        self.dependency
            .load_continuation(session_id, id)
            .map_err(ContinuationDataError::Dependency)
            .and_then(from_dependency_record)
    }

    fn resolve(
        &self,
        request: ResolveContinuationDataRequest,
    ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
        let target = if request.approved {
            DependencyContinuationState::Resumed
        } else {
            DependencyContinuationState::Cancelled
        };
        let response = self
            .dependency
            .transition_continuation(DependencyTransitionContinuationRequest {
                session_id: request.session_id,
                id: request.id,
                expected: DependencyContinuationState::Pending,
                target,
            })
            .map_err(ContinuationDataError::Dependency)?;
        Ok(ResolveContinuationDataRecord {
            transitioned: response.transitioned,
            state: from_dependency_state(response.current),
            payload: serde_json::from_slice(&response.payload_json)
                .map_err(ContinuationDataError::PayloadEncoding)?,
        })
    }
}

impl<D> ContinuationDataPort for super::RuntimeData<D>
where
    D: ContinuationDependencyPort,
{
    fn create(&self, request: CreateContinuationDataRequest) -> Result<(), ContinuationDataError> {
        if request.record.state != ContinuationStateRecord::Pending {
            return Err(ContinuationDataError::InvalidInitialState);
        }
        self.dependency
            .create_continuation(DependencyCreateContinuationRequest {
                record: to_dependency_record(request.record)?,
            })
            .map_err(ContinuationDataError::Dependency)
    }

    fn load(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<ContinuationRecord, ContinuationDataError> {
        self.dependency
            .load_continuation(session_id, id)
            .map_err(ContinuationDataError::Dependency)
            .and_then(from_dependency_record)
    }

    fn resolve(
        &self,
        request: ResolveContinuationDataRequest,
    ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
        let target = if request.approved {
            DependencyContinuationState::Resumed
        } else {
            DependencyContinuationState::Cancelled
        };
        let response = self
            .dependency
            .transition_continuation(DependencyTransitionContinuationRequest {
                session_id: request.session_id,
                id: request.id,
                expected: DependencyContinuationState::Pending,
                target,
            })
            .map_err(ContinuationDataError::Dependency)?;
        Ok(ResolveContinuationDataRecord {
            transitioned: response.transitioned,
            state: from_dependency_state(response.current),
            payload: serde_json::from_slice(&response.payload_json)
                .map_err(ContinuationDataError::PayloadEncoding)?,
        })
    }
}

fn to_dependency_record(
    record: ContinuationRecord,
) -> Result<DependencyContinuationRecord, ContinuationDataError> {
    Ok(DependencyContinuationRecord {
        session_id: record.session_id,
        id: record.id,
        state: to_dependency_state(record.state),
        wake_condition_json: serde_json::to_vec(&record.wake_condition)
            .map_err(ContinuationDataError::WakeEncoding)?,
        payload_json: serde_json::to_vec(&record.payload)
            .map_err(ContinuationDataError::PayloadEncoding)?,
        expires_at_millis: record.expires_at_millis,
    })
}

fn from_dependency_record(
    record: DependencyContinuationRecord,
) -> Result<ContinuationRecord, ContinuationDataError> {
    Ok(ContinuationRecord {
        session_id: record.session_id,
        id: record.id,
        state: from_dependency_state(record.state),
        wake_condition: serde_json::from_slice(&record.wake_condition_json)
            .map_err(ContinuationDataError::WakeEncoding)?,
        payload: serde_json::from_slice(&record.payload_json)
            .map_err(ContinuationDataError::PayloadEncoding)?,
        expires_at_millis: record.expires_at_millis,
    })
}

const fn to_dependency_state(state: ContinuationStateRecord) -> DependencyContinuationState {
    match state {
        ContinuationStateRecord::Pending => DependencyContinuationState::Pending,
        ContinuationStateRecord::Resumed => DependencyContinuationState::Resumed,
        ContinuationStateRecord::Cancelled => DependencyContinuationState::Cancelled,
        ContinuationStateRecord::Expired => DependencyContinuationState::Expired,
    }
}

const fn from_dependency_state(state: DependencyContinuationState) -> ContinuationStateRecord {
    match state {
        DependencyContinuationState::Pending => ContinuationStateRecord::Pending,
        DependencyContinuationState::Resumed => ContinuationStateRecord::Resumed,
        DependencyContinuationState::Cancelled => ContinuationStateRecord::Cancelled,
        DependencyContinuationState::Expired => ContinuationStateRecord::Expired,
    }
}

/// Continuation dataset failure.
#[derive(Debug, Error)]
pub enum ContinuationDataError {
    /// New records must begin pending.
    #[error("new continuation must be pending")]
    InvalidInitialState,
    /// Wake-condition serialization failed inside the data boundary.
    #[error("continuation wake condition is invalid: {0}")]
    WakeEncoding(#[source] serde_json::Error),
    /// Pending action serialization failed inside the data boundary.
    #[error("continuation payload is invalid: {0}")]
    PayloadEncoding(#[source] serde_json::Error),
    /// Persistence adapter failed.
    #[error("continuation persistence failed: {0}")]
    Dependency(#[source] ContinuationDependencyError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_dependency::continuation::DependencyTransitionContinuationResponse;

    use super::*;

    #[derive(Default)]
    struct MockDependency {
        created: RefCell<Vec<DependencyCreateContinuationRequest>>,
        transitioned: RefCell<Vec<DependencyTransitionContinuationRequest>>,
    }

    impl ContinuationDependencyPort for MockDependency {
        fn create_continuation(
            &self,
            request: DependencyCreateContinuationRequest,
        ) -> Result<(), ContinuationDependencyError> {
            self.created.borrow_mut().push(request);
            Ok(())
        }

        fn load_continuation(
            &self,
            session_id: &str,
            id: &str,
        ) -> Result<DependencyContinuationRecord, ContinuationDependencyError> {
            Ok(DependencyContinuationRecord {
                session_id: session_id.into(),
                id: id.into(),
                state: DependencyContinuationState::Pending,
                wake_condition_json: br#"{"kind":"manual"}"#.to_vec(),
                payload_json: br#"{"kind":"opaque","value":{"label":"fixture"}}"#.to_vec(),
                expires_at_millis: None,
            })
        }

        fn transition_continuation(
            &self,
            request: DependencyTransitionContinuationRequest,
        ) -> Result<DependencyTransitionContinuationResponse, ContinuationDependencyError> {
            self.transitioned.borrow_mut().push(request.clone());
            Ok(DependencyTransitionContinuationResponse {
                transitioned: true,
                current: request.target,
                payload_json: br#"{"kind":"opaque","value":{"label":"fixture"}}"#.to_vec(),
            })
        }
    }

    fn pending() -> ContinuationRecord {
        ContinuationRecord {
            session_id: "session_1".into(),
            id: "approval_1".into(),
            state: ContinuationStateRecord::Pending,
            wake_condition: ContinuationWakeRecord::Manual,
            payload: ContinuationPayloadRecord::Opaque {
                label: "fixture".into(),
            },
            expires_at_millis: None,
        }
    }

    #[test]
    fn maps_create_without_reusing_data_types() {
        let data = ContinuationData::new(MockDependency::default());
        data.create(CreateContinuationDataRequest { record: pending() })
            .expect("create");
        let observed = data.dependency.created.into_inner();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].record.id, "approval_1");
        assert_eq!(
            observed[0].record.state,
            DependencyContinuationState::Pending
        );
    }

    #[test]
    fn maps_approval_to_pending_compare_and_set() {
        let data = ContinuationData::new(MockDependency::default());
        let result = data
            .resolve(ResolveContinuationDataRequest {
                session_id: "session_1".into(),
                id: "approval_1".into(),
                approved: true,
            })
            .expect("resolve");
        assert!(result.transitioned);
        assert_eq!(result.state, ContinuationStateRecord::Resumed);
        assert_eq!(
            data.dependency.transitioned.into_inner(),
            vec![DependencyTransitionContinuationRequest {
                session_id: "session_1".into(),
                id: "approval_1".into(),
                expected: DependencyContinuationState::Pending,
                target: DependencyContinuationState::Resumed,
            }]
        );
    }

    #[test]
    fn normalizes_loaded_wake_condition() {
        let data = ContinuationData::new(MockDependency::default());
        assert_eq!(
            data.load("session_1", "approval_1")
                .expect("load")
                .wake_condition,
            ContinuationWakeRecord::Manual
        );
    }
}
