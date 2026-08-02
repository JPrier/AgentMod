//! Runtime business semantics for durable continuation creation and resolution.

use agentmod_primitives::{ContentHash, ContinuationId, TimestampMillis};
use agentmod_runtime_data::continuation::{
    ContinuationDataError, ContinuationDataPort, ContinuationPayloadRecord, ContinuationRecord,
    ContinuationStateRecord, ContinuationWakeRecord, CreateContinuationDataRequest,
    DeferredTurnPayloadRecord, MemoryWritePayloadRecord, PendingToolCallPayloadRecord,
    ResolveContinuationDataRequest, StyleApprovalPayloadRecord, ToolApprovalPayloadRecord,
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
    /// Durable wake condition used for scheduler proof validation.
    pub wake_condition: ContinuationWakeCondition,
    /// Optional expiration.
    pub expires_at: Option<TimestampMillis>,
    /// Pending action associated with the continuation.
    pub payload: ContinuationPayload,
}

/// Scheduler-owned proof that a durable wake condition matched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuationWakeProof {
    /// Time trigger observed at the claimed occurrence.
    At(TimestampMillis),
    /// Exact committed event type matched by the scheduler.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
        /// Scheduler observation timestamp.
        observed_at: TimestampMillis,
    },
    /// Exact process-output trigger matched by the scheduler.
    ProcessOutput {
        /// Runtime process identity.
        process_id: String,
        /// Literal bounded pattern.
        pattern: String,
        /// Scheduler observation timestamp.
        observed_at: TimestampMillis,
    },
}

/// Business command to claim a nonmanual continuation exactly once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeContinuationCommand {
    /// Session containing the continuation.
    pub session_id: String,
    /// Durable continuation.
    pub id: ContinuationId,
    /// Schedule presenting the claim.
    pub schedule_id: String,
    /// Authenticated trigger proof.
    pub proof: ContinuationWakeProof,
}

/// Business result for one durable wake attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeContinuationResult {
    /// True only for the request that won the resume-once transition.
    pub transitioned: bool,
    /// Deferred action associated with the continuation.
    pub payload: DeferredTurnContinuation,
}

/// Logic-owned durable pending action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuationPayload {
    /// Final intercepted tool call plus the turn state needed to continue.
    ToolApproval(Box<ToolApprovalContinuation>),
    /// A compiled style `user_approval` node plus exact command identity.
    StyleApproval(Box<StyleApprovalContinuation>),
    /// Complete provider turn deferred behind a scheduler-owned trigger.
    DeferredTurn(Box<DeferredTurnContinuation>),
    /// An approved automatic memory write waiting for its user decision.
    MemoryWrite(Box<MemoryWriteApprovalContinuation>),
    /// Storage-only marker for callers without an executable action.
    Opaque(String),
}

/// Logic-owned restart-safe compiled-style approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleApprovalContinuation {
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
    pub options: Value,
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

/// Logic-owned restart-safe automatic memory-write approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryWriteApprovalContinuation {
    /// Canonical session containing the pending write.
    pub session_id: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// Explicit session style.
    pub style: String,
    /// Stable cancellation identity for the owning execution.
    pub cancellation_id: String,
    /// Provider used for the write.
    pub provider: String,
    /// Normalized scope key.
    pub scope: String,
    /// Provenance label.
    pub source: String,
    /// Exact bounded content.
    pub content: String,
    /// Canonical duplicate-prevention key.
    pub deduplication_key: Option<String>,
    /// Canonical cross-restart write identity.
    pub write_id: String,
    /// Maximum retained bytes.
    pub max_bytes: u32,
    /// Trigger boundary that proposed the write.
    pub trigger: String,
    /// Hash of the exact content.
    pub content_hash: ContentHash,
}

/// Logic-owned restart-safe deferred provider turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredTurnContinuation {
    /// Canonical session containing the deferred work.
    pub session_id: String,
    /// Exact schedule allowed to claim this work.
    pub schedule_id: String,
    /// User-authored prompt.
    pub prompt: String,
    /// Workspace selected for the turn.
    pub workspace: String,
    /// Provider adapter selected for the turn.
    pub provider: String,
    /// Provider model selected for the turn.
    pub model: String,
    /// Provider-specific request options.
    pub options: Value,
    /// Session style selected for execution.
    pub style: String,
    /// Stable cancellation identifier for the deferred turn.
    pub cancellation_id: String,
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

    /// Claims a scheduler-owned, nonmanual continuation exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] when the proof, payload, or durable
    /// state does not authorize a wake.
    fn wake_continuation(
        &self,
        _command: WakeContinuationCommand,
    ) -> Result<WakeContinuationResult, ContinuationLogicError> {
        Err(ContinuationLogicError::InvalidWakeProof)
    }
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
        validate_expiration(command.expires_at, &command.wake_condition)?;
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
        let pending = self
            .data
            .load(&command.session_id, &command.id.to_string())
            .map_err(ContinuationLogicError::Data)?;
        if !matches!(pending.wake_condition, ContinuationWakeRecord::Manual) {
            return Err(ContinuationLogicError::InvalidWakeProof);
        }
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
            wake_condition: from_data_wake(record.wake_condition)?,
            expires_at: record.expires_at_millis.map(TimestampMillis::new),
            payload,
        })
    }

    fn wake_continuation(
        &self,
        command: WakeContinuationCommand,
    ) -> Result<WakeContinuationResult, ContinuationLogicError> {
        if command.session_id.trim().is_empty() || command.schedule_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        let loaded = self.load_continuation(LoadContinuationQuery {
            session_id: command.session_id.clone(),
            id: command.id,
        })?;
        let ContinuationPayload::DeferredTurn(payload) = loaded.payload else {
            return Err(ContinuationLogicError::InvalidPayload);
        };
        if payload.schedule_id != command.schedule_id
            || !wake_proof_matches(&loaded.wake_condition, &command.proof)
        {
            return Err(ContinuationLogicError::InvalidWakeProof);
        }
        if loaded
            .expires_at
            .is_some_and(|expiry| proof_timestamp(&command.proof) > expiry)
        {
            return Err(ContinuationLogicError::Expired);
        }
        match loaded.state {
            ContinuationState::Resumed => {
                return Ok(WakeContinuationResult {
                    transitioned: false,
                    payload: *payload,
                });
            }
            ContinuationState::Pending => {}
            ContinuationState::Cancelled | ContinuationState::Expired => {
                return Err(ContinuationLogicError::InvalidResolutionState);
            }
        }
        let resolved = self
            .data
            .resolve(ResolveContinuationDataRequest {
                session_id: command.session_id,
                id: command.id.to_string(),
                approved: true,
            })
            .map_err(ContinuationLogicError::Data)?;
        if resolved.state != ContinuationStateRecord::Resumed {
            return Err(ContinuationLogicError::InvalidResolutionState);
        }
        let ContinuationPayload::DeferredTurn(resolved_payload) =
            from_data_payload(resolved.payload)
        else {
            return Err(ContinuationLogicError::InvalidPayload);
        };
        Ok(WakeContinuationResult {
            transitioned: resolved.transitioned,
            payload: *resolved_payload,
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

fn validate_expiration(
    expires_at: Option<TimestampMillis>,
    wake_condition: &ContinuationWakeCondition,
) -> Result<(), ContinuationLogicError> {
    let Some(expires_at) = expires_at else {
        return Ok(());
    };
    if expires_at.get() < 0
        || matches!(wake_condition, ContinuationWakeCondition::At(wake_at) if wake_at > &expires_at)
    {
        return Err(ContinuationLogicError::InvalidExpiration);
    }
    Ok(())
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

fn from_data_wake(
    wake_condition: ContinuationWakeRecord,
) -> Result<ContinuationWakeCondition, ContinuationLogicError> {
    let wake_condition = match wake_condition {
        ContinuationWakeRecord::Manual => ContinuationWakeCondition::Manual,
        ContinuationWakeRecord::At(value) => {
            ContinuationWakeCondition::At(TimestampMillis::new(value))
        }
        ContinuationWakeRecord::RuntimeEvent {
            event_type,
            selector,
        } => ContinuationWakeCondition::RuntimeEvent {
            event_type,
            selector,
        },
        ContinuationWakeRecord::ProcessOutput {
            process_id,
            pattern,
        } => ContinuationWakeCondition::ProcessOutput {
            process_id,
            pattern,
        },
    };
    validate_wake_condition(&wake_condition)?;
    Ok(wake_condition)
}

fn wake_proof_matches(
    condition: &ContinuationWakeCondition,
    proof: &ContinuationWakeProof,
) -> bool {
    match (condition, proof) {
        (ContinuationWakeCondition::At(expected), ContinuationWakeProof::At(observed)) => {
            observed >= expected
        }
        (
            ContinuationWakeCondition::RuntimeEvent {
                event_type: expected,
                selector: None,
            },
            ContinuationWakeProof::RuntimeEvent { event_type, .. },
        ) => expected == event_type,
        (
            ContinuationWakeCondition::ProcessOutput {
                process_id: expected_process,
                pattern: expected_pattern,
            },
            ContinuationWakeProof::ProcessOutput {
                process_id,
                pattern,
                ..
            },
        ) => expected_process == process_id && expected_pattern == pattern,
        _ => false,
    }
}

fn proof_timestamp(proof: &ContinuationWakeProof) -> TimestampMillis {
    match proof {
        ContinuationWakeProof::At(timestamp) => *timestamp,
        ContinuationWakeProof::RuntimeEvent { observed_at, .. }
        | ContinuationWakeProof::ProcessOutput { observed_at, .. } => *observed_at,
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
        ContinuationPayload::StyleApproval(approval)
            if approval.session_id != session_id
                || approval.workspace.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.style.trim().is_empty()
                || approval.cancellation_id.trim().is_empty()
                || approval.compiled_style_cache_key.trim().is_empty()
                || approval.node_id.trim().is_empty()
                || approval.attempt == 0
                || approval.step == 0
                || approval.request_reference.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::DeferredTurn(turn)
            if turn.session_id != session_id
                || turn.schedule_id.trim().is_empty()
                || turn.prompt.trim().is_empty()
                || turn.workspace.trim().is_empty()
                || turn.provider.trim().is_empty()
                || turn.model.trim().is_empty()
                || !turn.options.is_object()
                || turn.style.trim().is_empty()
                || turn.cancellation_id.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::MemoryWrite(write)
            if write.session_id != session_id
                || write.workspace.trim().is_empty()
                || write.style.trim().is_empty()
                || write.cancellation_id.trim().is_empty()
                || write.provider.trim().is_empty()
                || write.scope.trim().is_empty()
                || write.source.trim().is_empty()
                || write.content.trim().is_empty()
                || write.write_id.trim().is_empty()
                || write.max_bytes == 0
                || write.trigger.trim().is_empty() =>
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
        ContinuationPayload::StyleApproval(approval) => {
            ContinuationPayloadRecord::StyleApproval(Box::new(StyleApprovalPayloadRecord {
                session_id: approval.session_id,
                workspace: approval.workspace,
                prompt: approval.prompt,
                provider: approval.provider,
                model: approval.model,
                options: approval.options,
                style: approval.style,
                cancellation_id: approval.cancellation_id,
                compiled_style_cache_key: approval.compiled_style_cache_key,
                node_id: approval.node_id,
                attempt: approval.attempt,
                loop_iteration: approval.loop_iteration,
                step: approval.step,
                request_reference: approval.request_reference,
            }))
        }
        ContinuationPayload::DeferredTurn(turn) => {
            ContinuationPayloadRecord::DeferredTurn(Box::new(DeferredTurnPayloadRecord {
                session_id: turn.session_id,
                schedule_id: turn.schedule_id,
                prompt: turn.prompt,
                workspace: turn.workspace,
                provider: turn.provider,
                model: turn.model,
                options: turn.options,
                style: turn.style,
                cancellation_id: turn.cancellation_id,
            }))
        }
        ContinuationPayload::MemoryWrite(write) => {
            ContinuationPayloadRecord::MemoryWrite(Box::new(MemoryWritePayloadRecord {
                session_id: write.session_id,
                workspace: write.workspace,
                style: write.style,
                cancellation_id: write.cancellation_id,
                provider: write.provider,
                scope: write.scope,
                source: write.source,
                content: write.content,
                deduplication_key: write.deduplication_key,
                write_id: write.write_id,
                max_bytes: write.max_bytes,
                trigger: write.trigger,
                content_hash: write.content_hash,
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
        ContinuationPayloadRecord::StyleApproval(approval) => {
            ContinuationPayload::StyleApproval(Box::new(StyleApprovalContinuation {
                session_id: approval.session_id,
                workspace: approval.workspace,
                prompt: approval.prompt,
                provider: approval.provider,
                model: approval.model,
                options: approval.options,
                style: approval.style,
                cancellation_id: approval.cancellation_id,
                compiled_style_cache_key: approval.compiled_style_cache_key,
                node_id: approval.node_id,
                attempt: approval.attempt,
                loop_iteration: approval.loop_iteration,
                step: approval.step,
                request_reference: approval.request_reference,
            }))
        }
        ContinuationPayloadRecord::DeferredTurn(turn) => {
            ContinuationPayload::DeferredTurn(Box::new(DeferredTurnContinuation {
                session_id: turn.session_id,
                schedule_id: turn.schedule_id,
                prompt: turn.prompt,
                workspace: turn.workspace,
                provider: turn.provider,
                model: turn.model,
                options: turn.options,
                style: turn.style,
                cancellation_id: turn.cancellation_id,
            }))
        }
        ContinuationPayloadRecord::MemoryWrite(write) => {
            ContinuationPayload::MemoryWrite(Box::new(MemoryWriteApprovalContinuation {
                session_id: write.session_id,
                workspace: write.workspace,
                style: write.style,
                cancellation_id: write.cancellation_id,
                provider: write.provider,
                scope: write.scope,
                source: write.source,
                content: write.content,
                deduplication_key: write.deduplication_key,
                write_id: write.write_id,
                max_bytes: write.max_bytes,
                trigger: write.trigger,
                content_hash: write.content_hash,
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
    /// Scheduler proof does not match the stored wake condition and schedule.
    #[error("continuation wake proof is invalid")]
    InvalidWakeProof,
    /// Continuation expired before the wake proof.
    #[error("continuation expired before wakeup")]
    Expired,
    /// Expiration cannot precede a time wake or the Unix epoch.
    #[error("continuation expiration is invalid")]
    InvalidExpiration,
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
            session_id: &str,
            id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            Ok(ContinuationRecord {
                session_id: session_id.to_owned(),
                id: id.to_owned(),
                state: ContinuationStateRecord::Pending,
                wake_condition: ContinuationWakeRecord::Manual,
                payload: ContinuationPayloadRecord::Opaque {
                    label: "fixture".into(),
                },
                expires_at_millis: None,
            })
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

    fn deferred_payload() -> ContinuationPayloadRecord {
        ContinuationPayloadRecord::DeferredTurn(Box::new(DeferredTurnPayloadRecord {
            session_id: "session_1".into(),
            schedule_id: "schedule_1".into(),
            prompt: "continue".into(),
            workspace: "workspace".into(),
            provider: "mock".into(),
            model: "mock-model".into(),
            options: serde_json::json!({}),
            style: "persistent-chat".into(),
            cancellation_id: Uuid::from_u128(2).to_string(),
        }))
    }

    struct WakeMockData {
        resolutions: RefCell<Vec<ResolveContinuationDataRequest>>,
        wake_condition: ContinuationWakeRecord,
        payload: ContinuationPayloadRecord,
        expires_at_millis: Option<i64>,
        state: ContinuationStateRecord,
        transitioned: bool,
    }

    impl ContinuationDataPort for WakeMockData {
        fn create(
            &self,
            _request: CreateContinuationDataRequest,
        ) -> Result<(), ContinuationDataError> {
            unreachable!("wake tests never create")
        }

        fn load(
            &self,
            session_id: &str,
            id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            Ok(ContinuationRecord {
                session_id: session_id.to_owned(),
                id: id.to_owned(),
                state: self.state,
                wake_condition: self.wake_condition.clone(),
                payload: self.payload.clone(),
                expires_at_millis: self.expires_at_millis,
            })
        }

        fn resolve(
            &self,
            request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            self.resolutions.borrow_mut().push(request);
            Ok(ResolveContinuationDataRecord {
                transitioned: self.transitioned,
                state: ContinuationStateRecord::Resumed,
                payload: self.payload.clone(),
            })
        }
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

    #[test]
    fn valid_schedule_proof_wakes_deferred_turn_once() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::RuntimeEvent {
                event_type: "task.ready".into(),
                selector: None,
            },
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        let result = logic
            .wake_continuation(WakeContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::RuntimeEvent {
                    event_type: "task.ready".into(),
                    observed_at: TimestampMillis::new(100),
                },
            })
            .expect("wake");
        assert!(result.transitioned);
        assert_eq!(result.payload.prompt, "continue");
        assert_eq!(logic.data.resolutions.borrow().len(), 1);
    }

    #[test]
    fn rejects_schedule_or_trigger_mismatch_without_transition() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::ProcessOutput {
                process_id: "process_1".into(),
                pattern: "ready".into(),
            },
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        for (schedule_id, pattern) in [("schedule_other", "ready"), ("schedule_1", "different")] {
            assert!(matches!(
                logic.wake_continuation(WakeContinuationCommand {
                    session_id: "session_1".into(),
                    id: id(),
                    schedule_id: schedule_id.into(),
                    proof: ContinuationWakeProof::ProcessOutput {
                        process_id: "process_1".into(),
                        pattern: pattern.into(),
                        observed_at: TimestampMillis::new(100),
                    },
                }),
                Err(ContinuationLogicError::InvalidWakeProof)
            ));
        }
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn resumed_continuation_is_an_idempotent_noop() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Resumed,
            transitioned: false,
        });
        let result = logic
            .wake_continuation(WakeContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::At(TimestampMillis::new(100)),
            })
            .expect("duplicate wake");
        assert!(!result.transitioned);
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn expired_time_proof_fails_before_transition() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: deferred_payload(),
            expires_at_millis: Some(199),
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        assert!(matches!(
            logic.wake_continuation(WakeContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::At(TimestampMillis::new(200)),
            }),
            Err(ContinuationLogicError::Expired)
        ));
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn manual_approval_cannot_resolve_scheduler_continuation() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        assert!(matches!(
            logic.resolve_approval(ResolveApprovalCommand {
                session_id: "session_1".into(),
                id: id(),
                approved: true,
            }),
            Err(ContinuationLogicError::InvalidWakeProof)
        ));
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn rejects_expiration_before_time_wake() {
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
                wake_condition: ContinuationWakeCondition::At(TimestampMillis::new(200)),
                payload: ContinuationPayload::Opaque("fixture".into()),
                expires_at: Some(TimestampMillis::new(199)),
            }),
            Err(ContinuationLogicError::InvalidExpiration)
        ));
        assert!(logic.data.creates.borrow().is_empty());
    }
}
