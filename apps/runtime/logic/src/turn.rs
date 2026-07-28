//! Durable runtime-owned session turn coordination.
#![allow(
    missing_docs,
    reason = "logic-local turn records are intentionally boundary-specific"
)]

use std::{
    collections::BTreeMap,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Weak},
};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{
    CausationId, ContinuationId, Sequence, SessionId, TimestampMillis, Version,
};
use agentmod_runtime_data::{
    continuation::ContinuationDataPort,
    harness::HarnessDataPort,
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataError, EventIdentityDataPort},
    journal::JournalEventDataPort,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};

use crate::{
    action::ConsequentialAction,
    continuation::{
        ApprovalDisposition, ContinuationLogic, ContinuationLogicPort, ContinuationPayload,
        ContinuationState, ContinuationWakeCondition, ContinuationWakeProof,
        CreateContinuationCommand, DeferredTurnContinuation, LoadContinuationQuery,
        PendingToolCallContinuation, ResolveApprovalCommand, ToolApprovalContinuation,
        WakeContinuationCommand,
    },
    conversation::{
        ConversationEntry, ConversationEntryId, TextEntry, ToolCallEntry, ToolResultEntry,
    },
    harness::{
        AuthorizedProviderRequest, ExecuteProviderCommand, ProviderEntry, ProviderEvent,
        ProviderEventStream, ProviderExecutionError, ProviderExecutionLogic,
        ProviderExecutionPolicy, ProviderExecutionPort,
    },
    persistence::{
        CommitDurability, CommitSessionEventCommand, LoadSessionCommand, SessionPersistenceLogic,
        SessionPersistenceLogicError, SessionPersistenceLogicPort,
    },
    session::{
        ApprovalRequestedEvent, ApprovalResolvedEvent, ApprovalState,
        ConversationEntryCommittedEvent, ModelOutputDeltaObservedEvent, ModelRequestApprovedEvent,
        ModelRequestCancelledEvent, ModelRequestFailedEvent, ModelRequestProposedEvent,
        ModelRequestStartedEvent, ModelResponseCompletedEvent, ModelToolCallDeltaObservedEvent,
        ModelToolCallProposedEvent, ProcessReconciliationCompletedEvent,
        ProcessReconciliationStartedEvent, ProcessReconciliationStatus, RuntimeCommittedEvent,
        SchedulerFiredEvent, SessionReducerError, ToolCallApprovedEvent, ToolCallProposedEvent,
        ToolExecutionCompletedEvent, ToolExecutionDispatchedEvent, ToolExecutionFailedEvent,
        ToolExecutionStartedEvent, ToolExecutionState, ToolOutputObservedEvent, reduce,
    },
    tool::{
        AuthorizedToolRequest, PrepareToolCommand, ToolAuthorizationOutcome, ToolEvent,
        ToolExecutionError, ToolExecutionLogic, ToolExecutionPolicy, ToolOutputStream,
    },
};

const MAX_PROMPT_BYTES: usize = 1024 * 1024;
const MAX_TOOL_STEPS: usize = 16;
const MAX_TOOL_RESULT_BYTES: usize = 64 * 1024;

fn process_reconciliation_id(action: &crate::action::ToolCallAction) -> Option<String> {
    (action.tool == "process.reattach")
        .then(|| {
            action
                .arguments
                .get("process_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
        })
        .flatten()
}

fn process_reconciliation_status(result: &Value) -> ProcessReconciliationStatus {
    match result.get("recovery_status").and_then(Value::as_str) {
        Some("live") => ProcessReconciliationStatus::Live,
        Some("recovered_running_unattached") => {
            ProcessReconciliationStatus::RecoveredRunningUnattached
        }
        Some("recovered_exited") => ProcessReconciliationStatus::RecoveredExited,
        Some("dispatch_uncertain") => ProcessReconciliationStatus::DispatchUncertain,
        _ => ProcessReconciliationStatus::Failed,
    }
}

#[derive(Clone, Copy)]
struct JournalPosition {
    sequence: Sequence,
    event_id: agentmod_primitives::EventId,
}

struct AuthorizedTurn {
    request: AuthorizedProviderRequest,
    position: JournalPosition,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunTurnCommand {
    pub sessions_root: PathBuf,
    pub session_id: String,
    pub prompt: String,
    pub provider: String,
    pub model: String,
    pub options: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunTurnResult {
    pub events: Vec<ProviderEvent>,
    pub first_committed_sequence: Sequence,
    pub last_committed_sequence: Sequence,
    pub awaiting_continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RunTurnStreamItem {
    Event {
        event: ProviderEvent,
        committed_sequence: Sequence,
    },
    Complete {
        first_committed_sequence: Sequence,
        last_committed_sequence: Sequence,
        awaiting_continuation: Option<String>,
    },
}

pub struct RunTurnStream {
    receiver: mpsc::Receiver<Result<RunTurnStreamItem, RunTurnError>>,
}

impl RunTurnStream {
    pub async fn next(&mut self) -> Option<Result<RunTurnStreamItem, RunTurnError>> {
        self.receiver.recv().await
    }
}

#[async_trait]
pub trait TurnLogicPort: Send + Sync {
    async fn run_turn(&self, command: RunTurnCommand) -> Result<RunTurnResult, RunTurnError>;
    async fn run_scheduled_turn(
        &self,
        command: RunScheduledTurnCommand,
    ) -> Result<RunTurnResult, RunTurnError> {
        self.run_turn(command.turn).await
    }
    async fn run_turn_stream(&self, command: RunTurnCommand)
    -> Result<RunTurnStream, RunTurnError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunScheduledTurnCommand {
    pub execution_id: String,
    pub schedule_id: String,
    pub scheduled_for_ms: i64,
    pub turn: RunTurnCommand,
}

struct ScheduledTurnPrelude {
    execution_id: String,
    schedule_id: String,
    scheduled_for_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CancelTurnCommand {
    pub cancellation_id: String,
    pub reason: String,
}

#[async_trait]
pub trait CancelTurnLogicPort: Send + Sync {
    async fn cancel_turn(&self, command: CancelTurnCommand) -> Result<(), RunTurnError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolveTurnApprovalCommand {
    pub sessions_root: PathBuf,
    pub session_id: String,
    pub continuation_id: String,
    pub approved: bool,
    pub resume_after_resolution: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolveTurnApprovalResult {
    pub transitioned: bool,
    pub events: Vec<ProviderEvent>,
    pub last_committed_sequence: Sequence,
    pub awaiting_continuation: Option<String>,
}

#[async_trait]
pub trait ApprovalTurnLogicPort: Send + Sync {
    async fn resolve_turn_approval(
        &self,
        command: ResolveTurnApprovalCommand,
    ) -> Result<ResolveTurnApprovalResult, RunTurnError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDeferredTurnCommand {
    pub session_id: String,
    pub continuation_id: String,
    pub schedule_id: String,
    pub prompt: String,
    pub workspace: String,
    pub provider: String,
    pub model: String,
    pub options: Value,
    pub style: String,
    pub cancellation_id: String,
    pub wake_condition: ContinuationWakeCondition,
    pub expires_at: Option<TimestampMillis>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeScheduledTurnCommand {
    pub sessions_root: PathBuf,
    pub session_id: String,
    pub continuation_id: String,
    pub execution_id: String,
    pub schedule_id: String,
    pub scheduled_for_ms: i64,
    pub proof: ContinuationWakeProof,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WakeScheduledTurnResult {
    pub transitioned: bool,
    pub turn: Option<RunTurnResult>,
}

#[async_trait]
pub trait DeferredTurnLogicPort: Send + Sync {
    /// Persists one exact schedule-bound turn continuation.
    ///
    /// # Errors
    ///
    /// Returns [`RunTurnError`] when identifiers, wake policy, or persistence
    /// are invalid.
    fn create_deferred_turn(&self, command: CreateDeferredTurnCommand) -> Result<(), RunTurnError>;

    /// Claims and executes a deferred turn after an authenticated scheduler wake.
    ///
    /// # Errors
    ///
    /// Returns [`RunTurnError`] when the wake proof is invalid or execution
    /// cannot enter the normal scheduled turn path.
    async fn wake_scheduled_turn(
        &self,
        command: WakeScheduledTurnCommand,
    ) -> Result<WakeScheduledTurnResult, RunTurnError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverStartupToolsCommand {
    pub sessions_root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverStartupToolsResult {
    pub receipt_count: usize,
    pub reconciled_count: usize,
    pub already_terminal_count: usize,
    pub deferred_approval_count: usize,
    pub orphaned_count: usize,
}

#[async_trait]
pub trait StartupToolRecoveryLogicPort: Send + Sync {
    async fn recover_startup_tools(
        &self,
        command: RecoverStartupToolsCommand,
    ) -> Result<RecoverStartupToolsResult, RunTurnError>;
}

enum ToolLoopOutcome {
    Complete {
        events: Vec<ProviderEvent>,
        position: JournalPosition,
    },
    Awaiting {
        events: Vec<ProviderEvent>,
        position: JournalPosition,
        continuation_id: ContinuationId,
    },
}

enum ToolCallOutcome {
    Complete(JournalPosition),
    Cancelled(JournalPosition),
    Awaiting {
        position: JournalPosition,
        continuation_id: ContinuationId,
    },
}

struct ToolExecutionResult {
    position: JournalPosition,
    cancelled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApprovalRecoveryAction {
    CommitAndResume,
    Resume,
    Reconcile,
    Idempotent,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolDispatchMode {
    Fresh,
    Reconcile { observed_event_count: u64 },
}

#[derive(Clone)]
pub struct TurnLogic<D> {
    data: D,
    provider: ProviderExecutionLogic<D>,
    tools: ToolExecutionLogic<D>,
    session_gates: Arc<Mutex<BTreeMap<String, Weak<Mutex<()>>>>>,
}

impl<D: Clone> TurnLogic<D> {
    #[must_use]
    pub fn new(data: D, policy: ProviderExecutionPolicy) -> Self {
        let tool_policy = ToolExecutionPolicy {
            style_pipeline: policy.style_pipeline.clone(),
            plugin_pipeline: policy.plugin_pipeline.clone(),
            user_policy: policy.user_policy.clone(),
            mandatory_policy: policy.mandatory_policy.clone(),
        };
        Self {
            provider: ProviderExecutionLogic::new(data.clone(), policy),
            tools: ToolExecutionLogic::new(data.clone(), tool_policy),
            data,
            session_gates: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    async fn session_gate(&self, session: &str) -> Arc<Mutex<()>> {
        let mut gates = self.session_gates.lock().await;
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(session).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        gates.insert(session.to_owned(), Arc::downgrade(&gate));
        gate
    }
}

#[async_trait]
impl<D> TurnLogicPort for TurnLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
{
    async fn run_turn(&self, command: RunTurnCommand) -> Result<RunTurnResult, RunTurnError> {
        self.run_turn_internal(command, None, None).await
    }

    async fn run_scheduled_turn(
        &self,
        command: RunScheduledTurnCommand,
    ) -> Result<RunTurnResult, RunTurnError> {
        if command.execution_id.len() != 64
            || !command
                .execution_id
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
            || command.schedule_id.trim().is_empty()
            || command.scheduled_for_ms < 0
        {
            return Err(RunTurnError::Invalid);
        }
        self.run_turn_internal(
            command.turn,
            None,
            Some(ScheduledTurnPrelude {
                execution_id: command.execution_id,
                schedule_id: command.schedule_id,
                scheduled_for_ms: command.scheduled_for_ms,
            }),
        )
        .await
    }

    async fn run_turn_stream(
        &self,
        command: RunTurnCommand,
    ) -> Result<RunTurnStream, RunTurnError> {
        validate(&command)?;
        let (sender, receiver) = mpsc::channel(16);
        let logic = self.clone();
        tokio::spawn(async move {
            match logic.run_turn_internal(command, Some(&sender), None).await {
                Ok(result) => {
                    let _ = sender
                        .send(Ok(RunTurnStreamItem::Complete {
                            first_committed_sequence: result.first_committed_sequence,
                            last_committed_sequence: result.last_committed_sequence,
                            awaiting_continuation: result.awaiting_continuation,
                        }))
                        .await;
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                }
            }
        });
        Ok(RunTurnStream { receiver })
    }
}

impl<D> TurnLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
{
    async fn run_turn_internal(
        &self,
        command: RunTurnCommand,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
        scheduled: Option<ScheduledTurnPrelude>,
    ) -> Result<RunTurnResult, RunTurnError> {
        validate(&command)?;
        let session_id =
            SessionId::from_str(&command.session_id).map_err(|_| RunTurnError::InvalidSession)?;
        let gate = self.session_gate(&command.session_id).await;
        let _session_guard = gate.lock().await;
        let session_directory = command.sessions_root.join(session_id.to_string());
        let persistence = SessionPersistenceLogic::new(self.data.clone());
        if let Some(scheduled) = scheduled {
            self.commit_scheduler_fired(&persistence, session_id, &session_directory, scheduled)?;
        }
        let (state, user_sequence, user_event) =
            self.commit_user(&persistence, session_id, &session_directory, &command)?;
        let authorized = self
            .authorize_and_commit(
                &persistence,
                session_id,
                &session_directory,
                JournalPosition {
                    sequence: user_sequence,
                    event_id: user_event.metadata.event_id,
                },
                state,
                &command,
            )
            .await?;
        let (events, observed) = self
            .execute_and_commit(
                &persistence,
                session_id,
                &session_directory,
                authorized,
                &command.cancellation_id,
                sink,
            )
            .await?;
        let tool_outcome = self
            .resolve_tool_calls(
                &persistence,
                session_id,
                &session_directory,
                &command,
                events,
                observed,
                sink,
            )
            .await?;
        let (events, observed) = match tool_outcome {
            ToolLoopOutcome::Complete { events, position } => (events, position),
            ToolLoopOutcome::Awaiting {
                events,
                position,
                continuation_id,
            } => {
                return Ok(RunTurnResult {
                    events,
                    first_committed_sequence: user_sequence,
                    last_committed_sequence: position.sequence,
                    awaiting_continuation: Some(continuation_id.to_string()),
                });
            }
        };
        let last_sequence = self.commit_visible_assistant(
            &persistence,
            session_id,
            session_directory,
            observed.sequence,
            observed.event_id,
            &command.cancellation_id,
            &events,
        )?;
        Ok(RunTurnResult {
            events,
            first_committed_sequence: user_sequence,
            last_committed_sequence: last_sequence,
            awaiting_continuation: None,
        })
    }
}

#[async_trait]
impl<D> CancelTurnLogicPort for TurnLogic<D>
where
    D: Clone + Send + Sync + HarnessDataPort + agentmod_runtime_data::tool::ToolDataPort,
{
    async fn cancel_turn(&self, command: CancelTurnCommand) -> Result<(), RunTurnError> {
        if command.cancellation_id.trim().is_empty() || command.reason.trim().is_empty() {
            return Err(RunTurnError::Invalid);
        }
        if self
            .tools
            .cancel(command.cancellation_id.clone())
            .await
            .map_err(RunTurnError::Tool)?
        {
            return Ok(());
        }
        let events = self
            .provider
            .cancel(command.cancellation_id)
            .await
            .map_err(RunTurnError::Provider)?;
        if events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Cancelled))
        {
            Ok(())
        } else {
            Err(RunTurnError::Provider(
                ProviderExecutionError::InvalidInterceptionReplacement,
            ))
        }
    }
}

#[async_trait]
impl<D> ApprovalTurnLogicPort for TurnLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + agentmod_runtime_data::tool::ToolDataPort,
{
    #[allow(clippy::too_many_lines)]
    async fn resolve_turn_approval(
        &self,
        command: ResolveTurnApprovalCommand,
    ) -> Result<ResolveTurnApprovalResult, RunTurnError> {
        let session_id =
            SessionId::from_str(&command.session_id).map_err(|_| RunTurnError::InvalidSession)?;
        let continuation_id = ContinuationId::from_str(&command.continuation_id)
            .map_err(|_| RunTurnError::InvalidContinuation)?;
        if command.approved && !command.resume_after_resolution {
            return Err(RunTurnError::Invalid);
        }
        let gate = self.session_gate(&command.session_id).await;
        let _session_guard = gate.lock().await;
        let session_directory = command.sessions_root.join(session_id.to_string());
        let persistence = SessionPersistenceLogic::new(self.data.clone());
        let continuation_logic = ContinuationLogic::new(self.data.clone());
        let loaded_continuation = continuation_logic
            .load_continuation(LoadContinuationQuery {
                session_id: command.session_id.clone(),
                id: continuation_id,
            })
            .map_err(RunTurnError::Continuation)?;
        if command.approved
            && matches!(
                loaded_continuation.state,
                ContinuationState::Pending | ContinuationState::Resumed
            )
        {
            let ContinuationPayload::ToolApproval(payload) = &loaded_continuation.payload else {
                return Err(RunTurnError::InvalidContinuationPayload);
            };
            self.tools
                .authorize_continuation_resume(
                    &command.session_id,
                    &payload.workspace,
                    &payload.style,
                    &command.continuation_id,
                )
                .await
                .map_err(RunTurnError::Tool)?;
        }
        let resolved = continuation_logic
            .resolve_approval(ResolveApprovalCommand {
                session_id: command.session_id.clone(),
                id: continuation_id,
                approved: command.approved,
            })
            .map_err(RunTurnError::Continuation)?;
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.clone(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        let ContinuationPayload::ToolApproval(payload_ref) = &resolved.payload else {
            return Err(RunTurnError::InvalidContinuationPayload);
        };
        let approval_state = loaded
            .state
            .approvals
            .get(&continuation_id)
            .map(|approval| approval.state)
            .ok_or(RunTurnError::InvalidContinuationPayload)?;
        let execution_record = loaded
            .state
            .tool_executions
            .get(&payload_ref.call_id)
            .cloned();
        let recovery = approval_recovery_action(
            resolved.transitioned,
            resolved.disposition,
            approval_state,
            execution_record.as_ref().map(|execution| execution.state),
        );
        let commit_resolution = match recovery {
            ApprovalRecoveryAction::CommitAndResume => true,
            ApprovalRecoveryAction::Resume | ApprovalRecoveryAction::Reconcile => false,
            ApprovalRecoveryAction::Idempotent => {
                return Ok(ResolveTurnApprovalResult {
                    transitioned: false,
                    events: Vec::new(),
                    last_committed_sequence: loaded.state.last_sequence,
                    awaiting_continuation: None,
                });
            }
            ApprovalRecoveryAction::Invalid => {
                return Err(RunTurnError::InvalidContinuationPayload);
            }
        };
        let mut position = JournalPosition {
            sequence: loaded.state.last_sequence,
            event_id: loaded.last_event_id,
        };
        if commit_resolution {
            (position.sequence, position.event_id) = self.commit_next(
                &persistence,
                session_id,
                &session_directory,
                position.sequence,
                position.event_id,
                RuntimeCommittedEvent::ApprovalResolved(ApprovalResolvedEvent {
                    continuation_id,
                    approved: command.approved,
                }),
            )?;
        }
        let ContinuationPayload::ToolApproval(payload) = resolved.payload else {
            return Err(RunTurnError::InvalidContinuationPayload);
        };
        if payload.session_id != command.session_id {
            return Err(RunTurnError::InvalidContinuationPayload);
        }
        let dispatch_mode = if recovery == ApprovalRecoveryAction::Reconcile {
            ToolDispatchMode::Reconcile {
                observed_event_count: execution_record
                    .as_ref()
                    .ok_or(RunTurnError::InvalidContinuationPayload)?
                    .observed_event_count,
            }
        } else {
            ToolDispatchMode::Fresh
        };
        let remaining_tool_calls = payload.remaining_tool_calls.clone();
        let resumed_turn = RunTurnCommand {
            sessions_root: command.sessions_root,
            session_id: command.session_id,
            prompt: String::new(),
            provider: payload.provider,
            model: payload.model,
            options: payload.options,
            cancellation_id: payload.cancellation_id,
        };
        let prepared = self
            .tools
            .prepare(PrepareToolCommand {
                session_id: resumed_turn.session_id.clone(),
                workspace: PathBuf::from(payload.workspace),
                call_id: payload.call_id.clone(),
                tool: payload.tool,
                arguments: payload.arguments,
                cancellation_id: resumed_turn.cancellation_id.clone(),
                style: payload.style,
            })
            .map_err(RunTurnError::Tool)?;
        let ConsequentialAction::ToolCall(action) = &prepared.original.action else {
            return Err(RunTurnError::InvalidContinuationPayload);
        };
        if resolved.disposition == ApprovalDisposition::Approved {
            let authorized = self
                .tools
                .approve_pending(PrepareToolCommand {
                    session_id: resumed_turn.session_id.clone(),
                    workspace: PathBuf::from(prepared.original.workspace.clone()),
                    call_id: payload.call_id.clone(),
                    tool: action.tool.clone(),
                    arguments: action.arguments.clone(),
                    cancellation_id: resumed_turn.cancellation_id.clone(),
                    style: prepared.original.style.clone(),
                })
                .map_err(RunTurnError::Tool)?;
            let tool_result = self
                .execute_authorized_tool(
                    &persistence,
                    session_id,
                    &session_directory,
                    position,
                    &payload.call_id,
                    authorized,
                    dispatch_mode,
                )
                .await?;
            position = tool_result.position;
            if tool_result.cancelled {
                let events = vec![ProviderEvent::Cancelled];
                let (last_committed_sequence, _) = self.commit_provider_events(
                    &persistence,
                    session_id,
                    &session_directory,
                    position.sequence,
                    position.event_id,
                    &resumed_turn.cancellation_id,
                    &events,
                )?;
                return Ok(ResolveTurnApprovalResult {
                    transitioned: true,
                    events,
                    last_committed_sequence,
                    awaiting_continuation: None,
                });
            }
        } else {
            (position.sequence, position.event_id) = self.commit_tool_failure(
                &persistence,
                session_id,
                &session_directory,
                position,
                &payload.call_id,
                "permission_denied",
                "user denied the requested action",
                false,
            )?;
            position = self.commit_tool_conversation(
                &persistence,
                session_id,
                &session_directory,
                position,
                &payload.call_id,
                action,
                &json!({"error":{"code":"permission_denied","message":"user denied the requested action"}}),
                None,
                false,
            )?;
        }
        if !command.resume_after_resolution {
            let events = vec![ProviderEvent::Cancelled];
            let (last_committed_sequence, _) = self.commit_provider_events(
                &persistence,
                session_id,
                &session_directory,
                position.sequence,
                position.event_id,
                &resumed_turn.cancellation_id,
                &events,
            )?;
            return Ok(ResolveTurnApprovalResult {
                transitioned: true,
                events,
                last_committed_sequence,
                awaiting_continuation: None,
            });
        }
        for (index, pending) in remaining_tool_calls.iter().cloned().enumerate() {
            let tail = remaining_tool_calls[index + 1..].to_vec();
            match self
                .execute_tool_call(
                    &persistence,
                    session_id,
                    &session_directory,
                    &resumed_turn,
                    position,
                    &pending.call_id,
                    &pending.tool,
                    pending.arguments,
                    &pending.harness_continuation,
                    tail,
                )
                .await?
            {
                ToolCallOutcome::Complete(next) => position = next,
                ToolCallOutcome::Cancelled(next) => {
                    let events = vec![ProviderEvent::Cancelled];
                    let (last_committed_sequence, _) = self.commit_provider_events(
                        &persistence,
                        session_id,
                        &session_directory,
                        next.sequence,
                        next.event_id,
                        &resumed_turn.cancellation_id,
                        &events,
                    )?;
                    return Ok(ResolveTurnApprovalResult {
                        transitioned: true,
                        events,
                        last_committed_sequence,
                        awaiting_continuation: None,
                    });
                }
                ToolCallOutcome::Awaiting {
                    position,
                    continuation_id,
                } => {
                    return Ok(ResolveTurnApprovalResult {
                        transitioned: true,
                        events: Vec::new(),
                        last_committed_sequence: position.sequence,
                        awaiting_continuation: Some(continuation_id.to_string()),
                    });
                }
            }
        }
        let state = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.clone(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?
            .state;
        let authorized = self
            .authorize_and_commit(
                &persistence,
                session_id,
                &session_directory,
                position,
                state,
                &resumed_turn,
            )
            .await?;
        let (events, observed) = self
            .execute_and_commit(
                &persistence,
                session_id,
                &session_directory,
                authorized,
                &resumed_turn.cancellation_id,
                None,
            )
            .await?;
        match self
            .resolve_tool_calls(
                &persistence,
                session_id,
                &session_directory,
                &resumed_turn,
                events,
                observed,
                None,
            )
            .await?
        {
            ToolLoopOutcome::Awaiting {
                events,
                position,
                continuation_id,
            } => Ok(ResolveTurnApprovalResult {
                transitioned: true,
                events,
                last_committed_sequence: position.sequence,
                awaiting_continuation: Some(continuation_id.to_string()),
            }),
            ToolLoopOutcome::Complete { events, position } => {
                let last_committed_sequence = self.commit_visible_assistant(
                    &persistence,
                    session_id,
                    session_directory,
                    position.sequence,
                    position.event_id,
                    &resumed_turn.cancellation_id,
                    &events,
                )?;
                Ok(ResolveTurnApprovalResult {
                    transitioned: true,
                    events,
                    last_committed_sequence,
                    awaiting_continuation: None,
                })
            }
        }
    }
}

#[async_trait]
impl<D> DeferredTurnLogicPort for TurnLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
{
    fn create_deferred_turn(&self, command: CreateDeferredTurnCommand) -> Result<(), RunTurnError> {
        let continuation_id = ContinuationId::from_str(&command.continuation_id)
            .map_err(|_| RunTurnError::InvalidContinuation)?;
        SessionId::from_str(&command.session_id).map_err(|_| RunTurnError::InvalidSession)?;
        ContinuationLogic::new(self.data.clone())
            .create_continuation(CreateContinuationCommand {
                session_id: command.session_id.clone(),
                id: continuation_id,
                wake_condition: command.wake_condition,
                payload: ContinuationPayload::DeferredTurn(Box::new(DeferredTurnContinuation {
                    session_id: command.session_id,
                    schedule_id: command.schedule_id,
                    prompt: command.prompt,
                    workspace: command.workspace,
                    provider: command.provider,
                    model: command.model,
                    options: command.options,
                    style: command.style,
                    cancellation_id: command.cancellation_id,
                })),
                expires_at: command.expires_at,
            })
            .map_err(RunTurnError::Continuation)
    }

    async fn wake_scheduled_turn(
        &self,
        command: WakeScheduledTurnCommand,
    ) -> Result<WakeScheduledTurnResult, RunTurnError> {
        let continuation_id = ContinuationId::from_str(&command.continuation_id)
            .map_err(|_| RunTurnError::InvalidContinuation)?;
        let wake = ContinuationLogic::new(self.data.clone())
            .wake_continuation(WakeContinuationCommand {
                session_id: command.session_id.clone(),
                id: continuation_id,
                schedule_id: command.schedule_id.clone(),
                proof: command.proof,
            })
            .map_err(RunTurnError::Continuation)?;
        if !wake.transitioned {
            return Ok(WakeScheduledTurnResult {
                transitioned: false,
                turn: None,
            });
        }
        let turn = self
            .run_scheduled_turn(RunScheduledTurnCommand {
                execution_id: command.execution_id,
                schedule_id: command.schedule_id,
                scheduled_for_ms: command.scheduled_for_ms,
                turn: RunTurnCommand {
                    sessions_root: command.sessions_root,
                    session_id: wake.payload.session_id,
                    prompt: wake.payload.prompt,
                    provider: wake.payload.provider,
                    model: wake.payload.model,
                    options: wake.payload.options,
                    cancellation_id: wake.payload.cancellation_id,
                },
            })
            .await?;
        Ok(WakeScheduledTurnResult {
            transitioned: true,
            turn: Some(turn),
        })
    }
}

#[async_trait]
impl<D> StartupToolRecoveryLogicPort for TurnLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + agentmod_runtime_data::tool::ToolDataPort,
{
    #[allow(
        clippy::too_many_lines,
        reason = "startup recovery keeps one auditable receipt-to-journal transaction flow"
    )]
    async fn recover_startup_tools(
        &self,
        command: RecoverStartupToolsCommand,
    ) -> Result<RecoverStartupToolsResult, RunTurnError> {
        if command.sessions_root.as_os_str().is_empty() {
            return Err(RunTurnError::Invalid);
        }
        let receipts = self.data.list_tool_receipts().map_err(|error| {
            RunTurnError::Tool(match error {
                agentmod_runtime_data::tool::ToolDataError::ReceiptUnavailable => {
                    ToolExecutionError::ReceiptUnavailable
                }
                agentmod_runtime_data::tool::ToolDataError::Unavailable => {
                    ToolExecutionError::Unavailable
                }
            })
        })?;
        let receipt_count = receipts.len();
        let mut result = RecoverStartupToolsResult {
            receipt_count,
            reconciled_count: 0,
            already_terminal_count: 0,
            deferred_approval_count: 0,
            orphaned_count: 0,
        };
        let persistence = SessionPersistenceLogic::new(self.data.clone());
        let continuations = ContinuationLogic::new(self.data.clone());
        for receipt in receipts {
            let session_id = SessionId::from_str(&receipt.session_id)
                .map_err(|_| RunTurnError::InvalidRecoveryReceipt(receipt.call_id.clone()))?;
            let gate = self.session_gate(&receipt.session_id).await;
            let _session_guard = gate.lock().await;
            let session_directory = command.sessions_root.join(session_id.to_string());
            let loaded = persistence
                .load_session(LoadSessionCommand {
                    session_directory: session_directory.clone(),
                    expected_session_id: session_id,
                })
                .map_err(RunTurnError::Persistence)?;
            let Some(execution) = loaded.state.tool_executions.get(&receipt.call_id) else {
                result.orphaned_count += 1;
                continue;
            };
            if execution.state == ToolExecutionState::Terminal {
                result.already_terminal_count += 1;
                continue;
            }
            let mut approval_owned = false;
            for approval in loaded
                .state
                .approvals
                .values()
                .filter(|approval| approval.state == ApprovalState::Approved)
            {
                let continuation = continuations
                    .load_continuation(LoadContinuationQuery {
                        session_id: receipt.session_id.clone(),
                        id: approval.continuation_id,
                    })
                    .map_err(RunTurnError::Continuation)?;
                if matches!(
                    continuation.payload,
                    ContinuationPayload::ToolApproval(ref payload)
                        if payload.call_id == receipt.call_id
                ) {
                    approval_owned = true;
                    break;
                }
            }
            if approval_owned {
                result.deferred_approval_count += 1;
                continue;
            }
            if execution.execution_id != receipt.execution_id {
                return Err(RunTurnError::InvalidRecoveryReceipt(receipt.call_id));
            }
            let authorized = self
                .tools
                .approve_pending(PrepareToolCommand {
                    session_id: receipt.session_id,
                    workspace: receipt.workspace,
                    call_id: receipt.call_id.clone(),
                    tool: receipt.tool,
                    arguments: receipt.arguments,
                    cancellation_id: receipt.cancellation_id,
                    style: loaded.state.style.clone(),
                })
                .map_err(RunTurnError::Tool)?;
            let digest = authorized
                .executable
                .digest()
                .map_err(|_| RunTurnError::Event)?;
            if digest != execution.action_digest
                || authorized.original.id.0 != execution.execution_id
            {
                return Err(RunTurnError::InvalidRecoveryReceipt(receipt.call_id));
            }
            self.execute_authorized_tool(
                &persistence,
                session_id,
                &session_directory,
                JournalPosition {
                    sequence: loaded.state.last_sequence,
                    event_id: loaded.last_event_id,
                },
                &execution.call_id,
                authorized,
                ToolDispatchMode::Reconcile {
                    observed_event_count: execution.observed_event_count,
                },
            )
            .await?;
            result.reconciled_count += 1;
        }
        Ok(result)
    }
}

impl<D> TurnLogic<D>
where
    D: Clone
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + agentmod_runtime_data::tool::ToolDataPort,
{
    async fn authorize_and_commit(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        position: JournalPosition,
        state: crate::session::SessionState,
        command: &RunTurnCommand,
    ) -> Result<AuthorizedTurn, RunTurnError> {
        let prepared = self
            .provider
            .prepare(ExecuteProviderCommand {
                session_id: command.session_id.clone(),
                provider: command.provider.clone(),
                model: command.model.clone(),
                entries: project(state.conversation.provider_projection()),
                options: command.options.clone(),
                cancellation_id: command.cancellation_id.clone(),
                style: state.style,
                workspace: state.workspace,
            })
            .map_err(RunTurnError::Provider)?;
        let ConsequentialAction::ModelRequest(original) = &prepared.original.action else {
            return Err(invalid_replacement());
        };
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ModelRequestProposed(ModelRequestProposedEvent {
                proposal_id: prepared.original.id.0.clone(),
                provider: original.provider.clone(),
                model: original.model.clone(),
                projection_hash: original.projection_hash,
            }),
        )?;
        let request = match self.provider.authorize_prepared(prepared).await {
            Ok(request) => request,
            Err(error) => {
                if let Err(audit_error) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    sequence,
                    event_id,
                    RuntimeCommittedEvent::ModelRequestFailed(ModelRequestFailedEvent {
                        code: "authorization".into(),
                        message: error.to_string(),
                        retryable: false,
                    }),
                ) {
                    return Err(RunTurnError::ProviderFailureAudit {
                        provider: error.to_string(),
                        audit: audit_error.to_string(),
                    });
                }
                return Err(RunTurnError::Provider(error));
            }
        };
        let ConsequentialAction::ModelRequest(executable) = &request.executable.action else {
            return Err(invalid_replacement());
        };
        let action_digest = request
            .executable
            .digest()
            .map_err(|_| RunTurnError::Event)?;
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            sequence,
            event_id,
            RuntimeCommittedEvent::ModelRequestApproved(ModelRequestApprovedEvent {
                proposal_id: request.original.id.0.clone(),
                provider: executable.provider.clone(),
                model: executable.model.clone(),
                action_digest,
            }),
        )?;
        Ok(AuthorizedTurn {
            request,
            position: JournalPosition { sequence, event_id },
        })
    }

    async fn execute_and_commit(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        authorized: AuthorizedTurn,
        cancellation_id: &str,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
    ) -> Result<(Vec<ProviderEvent>, JournalPosition), RunTurnError> {
        let start = authorized.position;
        let stream = match self
            .provider
            .execute_authorized_stream(authorized.request)
            .await
        {
            Ok(stream) => stream,
            Err(error) => {
                if let Err(audit_error) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    start.sequence,
                    start.event_id,
                    RuntimeCommittedEvent::ModelRequestFailed(ModelRequestFailedEvent {
                        code: "harness_execution".into(),
                        message: error.to_string(),
                        retryable: false,
                    }),
                ) {
                    return Err(RunTurnError::ProviderFailureAudit {
                        provider: error.to_string(),
                        audit: audit_error.to_string(),
                    });
                }
                return Err(RunTurnError::Provider(error));
            }
        };
        self.drain_provider_stream(
            stream,
            persistence,
            session_id,
            session_directory,
            start,
            cancellation_id,
            sink,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "tool-loop coordination keeps each durable authority and stream sink explicit"
    )]
    async fn resolve_tool_calls(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        command: &RunTurnCommand,
        mut current_events: Vec<ProviderEvent>,
        mut position: JournalPosition,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
    ) -> Result<ToolLoopOutcome, RunTurnError> {
        let mut all_events = current_events.clone();
        for _ in 0..MAX_TOOL_STEPS {
            let proposals: Vec<_> = current_events
                .iter()
                .filter_map(|event| match event {
                    ProviderEvent::ToolProposed {
                        continuation_id,
                        call_id,
                        tool,
                        arguments,
                    } => Some((
                        continuation_id.clone(),
                        call_id.clone(),
                        tool.clone(),
                        arguments.clone(),
                    )),
                    _ => None,
                })
                .collect();
            if proposals.is_empty() {
                return Ok(ToolLoopOutcome::Complete {
                    events: all_events,
                    position,
                });
            }
            let resume_continuation = proposals[0].0.clone();
            for (index, (continuation_id, call_id, tool, arguments)) in
                proposals.iter().cloned().enumerate()
            {
                let remaining_tool_calls = proposals[index + 1..]
                    .iter()
                    .map(|(harness_continuation, call_id, tool, arguments)| {
                        PendingToolCallContinuation {
                            harness_continuation: harness_continuation.clone(),
                            call_id: call_id.clone(),
                            tool: tool.clone(),
                            arguments: arguments.clone(),
                        }
                    })
                    .collect();
                match self
                    .execute_tool_call(
                        persistence,
                        session_id,
                        session_directory,
                        command,
                        position,
                        &call_id,
                        &tool,
                        arguments,
                        &continuation_id,
                        remaining_tool_calls,
                    )
                    .await?
                {
                    ToolCallOutcome::Complete(next) => position = next,
                    ToolCallOutcome::Cancelled(next) => {
                        return self
                            .complete_cancelled_tool(
                                persistence,
                                session_id,
                                session_directory,
                                &command.cancellation_id,
                                next,
                                sink,
                                all_events,
                            )
                            .await;
                    }
                    ToolCallOutcome::Awaiting {
                        position,
                        continuation_id,
                    } => {
                        return Ok(ToolLoopOutcome::Awaiting {
                            events: all_events,
                            position,
                            continuation_id,
                        });
                    }
                }
            }
            let loaded = persistence
                .load_session(LoadSessionCommand {
                    session_directory: session_directory.to_owned(),
                    expected_session_id: session_id,
                })
                .map_err(RunTurnError::Persistence)?;
            let stream = self
                .provider
                .continue_execution_stream(
                    resume_continuation,
                    crate::harness::ProviderDecision::Replace(project(
                        loaded.state.conversation.provider_projection(),
                    )),
                )
                .await
                .map_err(RunTurnError::Provider)?;
            (current_events, position) = self
                .drain_provider_stream(
                    stream,
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    &command.cancellation_id,
                    sink,
                )
                .await?;
            all_events.extend(current_events.clone());
        }
        Err(RunTurnError::ToolStepLimit)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical cancellation keeps journal identity and optional stream delivery explicit"
    )]
    async fn complete_cancelled_tool(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        cancellation_id: &str,
        position: JournalPosition,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
        mut events: Vec<ProviderEvent>,
    ) -> Result<ToolLoopOutcome, RunTurnError> {
        let event = ProviderEvent::Cancelled;
        let (sequence, event_id) = self.commit_provider_events(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            cancellation_id,
            std::slice::from_ref(&event),
        )?;
        if let Some(sink) = sink {
            let _ = sink
                .send(Ok(RunTurnStreamItem::Event {
                    event: event.clone(),
                    committed_sequence: sequence,
                }))
                .await;
        }
        events.push(event);
        Ok(ToolLoopOutcome::Complete {
            events,
            position: JournalPosition { sequence, event_id },
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "stream draining explicitly binds each event to its session journal and frontend sink"
    )]
    async fn drain_provider_stream(
        &self,
        mut stream: ProviderEventStream,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut position: JournalPosition,
        cancellation_id: &str,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
    ) -> Result<(Vec<ProviderEvent>, JournalPosition), RunTurnError> {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    if let Err(audit_error) = self.commit_next(
                        persistence,
                        session_id,
                        session_directory,
                        position.sequence,
                        position.event_id,
                        RuntimeCommittedEvent::ModelRequestFailed(ModelRequestFailedEvent {
                            code: "harness_stream".into(),
                            message: error.to_string(),
                            retryable: false,
                        }),
                    ) {
                        return Err(RunTurnError::ProviderFailureAudit {
                            provider: error.to_string(),
                            audit: audit_error.to_string(),
                        });
                    }
                    return Err(RunTurnError::Provider(error));
                }
            };
            let (sequence, event_id) = self.commit_provider_events(
                persistence,
                session_id,
                session_directory,
                position.sequence,
                position.event_id,
                cancellation_id,
                std::slice::from_ref(&event),
            )?;
            position = JournalPosition { sequence, event_id };
            if let Some(sink) = sink {
                let _ = sink
                    .send(Ok(RunTurnStreamItem::Event {
                        event: event.clone(),
                        committed_sequence: sequence,
                    }))
                    .await;
            }
            events.push(event);
        }
        Ok((events, position))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn execute_tool_call(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        command: &RunTurnCommand,
        mut position: JournalPosition,
        call_id: &str,
        tool: &str,
        arguments: Value,
        harness_continuation: &str,
        remaining_tool_calls: Vec<PendingToolCallContinuation>,
    ) -> Result<ToolCallOutcome, RunTurnError> {
        let state = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?
            .state;
        let prepared = self
            .tools
            .prepare(PrepareToolCommand {
                session_id: command.session_id.clone(),
                workspace: PathBuf::from(state.workspace),
                call_id: call_id.to_owned(),
                tool: tool.to_owned(),
                arguments,
                cancellation_id: command.cancellation_id.clone(),
                style: state.style,
            })
            .map_err(RunTurnError::Tool)?;
        let ConsequentialAction::ToolCall(original_action) = &prepared.original.action else {
            return Err(RunTurnError::Tool(ToolExecutionError::InvalidReplacement));
        };
        let original_action = original_action.clone();
        (position.sequence, position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ToolCallProposed(ToolCallProposedEvent {
                proposal_id: prepared.original.id.0.clone(),
                call_id: call_id.to_owned(),
                tool: original_action.tool.clone(),
                arguments: original_action.arguments.clone(),
            }),
        )?;
        let authorized = match self.tools.authorize_prepared_outcome(prepared).await {
            Ok(ToolAuthorizationOutcome::Authorized(authorized)) => authorized,
            Ok(ToolAuthorizationOutcome::ApprovalRequired { pending, reason }) => {
                let ConsequentialAction::ToolCall(executable) = &pending.executable.action else {
                    return Err(RunTurnError::Tool(ToolExecutionError::InvalidReplacement));
                };
                let continuation_id = ContinuationId::from_uuid(position.event_id.into_uuid());
                ContinuationLogic::new(self.data.clone())
                    .create_continuation(CreateContinuationCommand {
                        session_id: command.session_id.clone(),
                        id: continuation_id,
                        wake_condition: ContinuationWakeCondition::Manual,
                        payload: ContinuationPayload::ToolApproval(Box::new(
                            ToolApprovalContinuation {
                                session_id: command.session_id.clone(),
                                workspace: pending.executable.workspace.clone(),
                                call_id: call_id.to_owned(),
                                tool: executable.tool.clone(),
                                arguments: executable.arguments.clone(),
                                cancellation_id: command.cancellation_id.clone(),
                                provider: command.provider.clone(),
                                model: command.model.clone(),
                                options: command.options.clone(),
                                style: pending.executable.style.clone(),
                                harness_continuation: harness_continuation.to_owned(),
                                remaining_tool_calls,
                            },
                        )),
                        expires_at: None,
                    })
                    .map_err(RunTurnError::Continuation)?;
                (position.sequence, position.event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    position.sequence,
                    position.event_id,
                    RuntimeCommittedEvent::ApprovalRequested(ApprovalRequestedEvent {
                        continuation_id,
                        action_summary: reason,
                    }),
                )?;
                return Ok(ToolCallOutcome::Awaiting {
                    position,
                    continuation_id,
                });
            }
            Err(error) => {
                (position.sequence, position.event_id) = self.commit_tool_failure(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    call_id,
                    "authorization",
                    &error.to_string(),
                    false,
                )?;
                let position = self.commit_tool_conversation(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    call_id,
                    &original_action,
                    &json!({"error":{"code":"authorization","message":error.to_string()}}),
                    None,
                    false,
                )?;
                return Ok(ToolCallOutcome::Complete(position));
            }
        };
        let result = self
            .execute_authorized_tool(
                persistence,
                session_id,
                session_directory,
                position,
                call_id,
                authorized,
                ToolDispatchMode::Fresh,
            )
            .await?;
        Ok(if result.cancelled {
            ToolCallOutcome::Cancelled(result.position)
        } else {
            ToolCallOutcome::Complete(result.position)
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn execute_authorized_tool(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut position: JournalPosition,
        call_id: &str,
        authorized: AuthorizedToolRequest,
        dispatch_mode: ToolDispatchMode,
    ) -> Result<ToolExecutionResult, RunTurnError> {
        let ConsequentialAction::ToolCall(executable) = &authorized.executable.action else {
            return Err(RunTurnError::Tool(ToolExecutionError::InvalidReplacement));
        };
        let executable = executable.clone();
        let reconciliation_process_id = process_reconciliation_id(&executable);
        let action_digest = authorized
            .executable
            .digest()
            .map_err(|_| RunTurnError::Event)?;
        let execution_id = authorized.original.id.0.clone();
        let mut reconciliation_completed = false;
        if dispatch_mode == ToolDispatchMode::Fresh {
            (position.sequence, position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                position.sequence,
                position.event_id,
                RuntimeCommittedEvent::ToolCallApproved(ToolCallApprovedEvent {
                    proposal_id: execution_id.clone(),
                    call_id: call_id.to_owned(),
                    action_digest,
                }),
            )?;
            if let Some(process_id) = &reconciliation_process_id {
                (position.sequence, position.event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    position.sequence,
                    position.event_id,
                    RuntimeCommittedEvent::ProcessReconciliationStarted(
                        ProcessReconciliationStartedEvent {
                            call_id: call_id.to_owned(),
                            process_id: process_id.clone(),
                        },
                    ),
                )?;
            }
            (position.sequence, position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                position.sequence,
                position.event_id,
                RuntimeCommittedEvent::ToolExecutionDispatched(ToolExecutionDispatchedEvent {
                    execution_id,
                    call_id: call_id.to_owned(),
                    action_digest,
                }),
            )?;
        } else if let Some(process_id) = &reconciliation_process_id {
            let state = persistence
                .load_session(LoadSessionCommand {
                    session_directory: session_directory.to_owned(),
                    expected_session_id: session_id,
                })
                .map_err(RunTurnError::Persistence)?
                .state;
            if let Some(record) = state.process_reconciliations.get(call_id) {
                if record.process_id != *process_id {
                    return Err(RunTurnError::InvalidContinuationPayload);
                }
                reconciliation_completed = record.completed_at.is_some();
            } else {
                (position.sequence, position.event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    position.sequence,
                    position.event_id,
                    RuntimeCommittedEvent::ProcessReconciliationStarted(
                        ProcessReconciliationStartedEvent {
                            call_id: call_id.to_owned(),
                            process_id: process_id.clone(),
                        },
                    ),
                )?;
            }
        }
        let receipt_only = matches!(dispatch_mode, ToolDispatchMode::Reconcile { .. });
        let tool_events = match self
            .tools
            .execute_authorized(authorized, receipt_only)
            .await
        {
            Ok(events) => events,
            Err(ToolExecutionError::ReceiptUnavailable) => {
                return Err(RunTurnError::AmbiguousToolExecution(call_id.to_owned()));
            }
            Err(error) => {
                if let Some(process_id) = &reconciliation_process_id
                    && !reconciliation_completed
                {
                    (position.sequence, position.event_id) = self
                        .commit_process_reconciliation_completed(
                            persistence,
                            session_id,
                            session_directory,
                            position,
                            call_id,
                            process_id,
                            ProcessReconciliationStatus::Failed,
                        )?;
                }
                (position.sequence, position.event_id) = self.commit_tool_failure(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    call_id,
                    "host_unavailable",
                    &error.to_string(),
                    true,
                )?;
                return self
                    .commit_tool_conversation(
                        persistence,
                        session_id,
                        session_directory,
                        position,
                        call_id,
                        &executable,
                        &json!({"error":{"code":"host_unavailable","message":error.to_string()}}),
                        None,
                        false,
                    )
                    .map(|position| ToolExecutionResult {
                        position,
                        cancelled: false,
                    });
            }
        };
        let mut final_result = json!({"error":{"code":"missing_terminal_event"}});
        let mut artifact = None;
        let mut truncated = false;
        let mut cancelled = false;
        let output_process_id = if executable.tool.starts_with("process.") {
            tool_events
                .iter()
                .find_map(|event| match event {
                    ToolEvent::Completed { result, .. } => result
                        .get("process_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    _ => None,
                })
                .or_else(|| {
                    executable
                        .arguments
                        .get("process_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
        } else {
            None
        };
        let output_range = if executable.tool == "process.read" {
            executable
                .arguments
                .get("offset")
                .and_then(Value::as_u64)
                .zip(tool_events.iter().find_map(|event| match event {
                    ToolEvent::Completed { result, .. } => {
                        result.get("next_offset").and_then(Value::as_u64)
                    }
                    _ => None,
                }))
        } else {
            None
        };
        let output_source_stream = if executable.tool == "process.read" {
            executable
                .arguments
                .get("stream")
                .and_then(Value::as_str)
                .map(str::to_owned)
        } else {
            None
        };
        let observed_event_count = match dispatch_mode {
            ToolDispatchMode::Fresh => 0,
            ToolDispatchMode::Reconcile {
                observed_event_count,
            } => observed_event_count,
        };
        let observed_event_count = usize::try_from(observed_event_count)
            .map_err(|_| RunTurnError::InvalidContinuationPayload)?;
        if observed_event_count > tool_events.len()
            || (receipt_only && observed_event_count == tool_events.len())
        {
            return Err(RunTurnError::InvalidContinuationPayload);
        }
        for event in tool_events.into_iter().skip(observed_event_count) {
            if let Some(process_id) = &reconciliation_process_id
                && !reconciliation_completed
            {
                let status = match &event {
                    ToolEvent::Completed { result, .. } => {
                        Some(process_reconciliation_status(result))
                    }
                    ToolEvent::Failed { .. } | ToolEvent::Cancelled { .. } => {
                        Some(ProcessReconciliationStatus::Failed)
                    }
                    ToolEvent::Started { .. }
                    | ToolEvent::Progress { .. }
                    | ToolEvent::Output { .. } => None,
                };
                if let Some(status) = status {
                    (position.sequence, position.event_id) = self
                        .commit_process_reconciliation_completed(
                            persistence,
                            session_id,
                            session_directory,
                            position,
                            call_id,
                            process_id,
                            status,
                        )?;
                    reconciliation_completed = true;
                }
            }
            let payload = match event {
                ToolEvent::Started { call_id } => {
                    RuntimeCommittedEvent::ToolExecutionStarted(ToolExecutionStartedEvent {
                        call_id,
                    })
                }
                ToolEvent::Progress {
                    call_id,
                    message,
                    completed,
                    total,
                } => RuntimeCommittedEvent::ToolOutputObserved(ToolOutputObservedEvent {
                    call_id,
                    process_id: output_process_id.clone(),
                    source_stream: None,
                    source_offset: None,
                    source_end: None,
                    stream: "progress".into(),
                    content: format!("{message} ({completed:?}/{total:?})"),
                }),
                ToolEvent::Output {
                    call_id,
                    stream,
                    content,
                } => RuntimeCommittedEvent::ToolOutputObserved(ToolOutputObservedEvent {
                    call_id,
                    process_id: output_process_id.clone(),
                    source_stream: output_source_stream.clone(),
                    source_offset: output_range.map(|(start, _)| start),
                    source_end: output_range.map(|(_, end)| end),
                    stream: match stream {
                        ToolOutputStream::Standard => "standard",
                        ToolOutputStream::Error => "error",
                    }
                    .into(),
                    content,
                }),
                ToolEvent::Completed {
                    call_id,
                    result,
                    artifact: result_artifact,
                    truncated: result_truncated,
                } => {
                    final_result = result.clone();
                    artifact = result_artifact;
                    truncated = result_truncated;
                    RuntimeCommittedEvent::ToolExecutionCompleted(ToolExecutionCompletedEvent {
                        call_id,
                        result,
                        artifact: artifact.clone(),
                        truncated,
                    })
                }
                ToolEvent::Failed {
                    call_id,
                    code,
                    message,
                    retryable,
                } => {
                    final_result =
                        json!({"error":{"code":code,"message":message,"retryable":retryable}});
                    RuntimeCommittedEvent::ToolExecutionFailed(ToolExecutionFailedEvent {
                        call_id,
                        code,
                        message,
                        retryable,
                    })
                }
                ToolEvent::Cancelled { call_id } => {
                    cancelled = true;
                    final_result = json!({"error":{"code":"cancelled"}});
                    RuntimeCommittedEvent::ToolExecutionFailed(ToolExecutionFailedEvent {
                        call_id,
                        code: "cancelled".into(),
                        message: "tool execution was cancelled".into(),
                        retryable: false,
                    })
                }
            };
            (position.sequence, position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                position.sequence,
                position.event_id,
                payload,
            )?;
        }
        self.commit_tool_conversation(
            persistence,
            session_id,
            session_directory,
            position,
            call_id,
            &executable,
            &final_result,
            artifact.as_deref(),
            truncated,
        )
        .map(|position| ToolExecutionResult {
            position,
            cancelled,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_process_reconciliation_completed(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        position: JournalPosition,
        call_id: &str,
        process_id: &str,
        status: ProcessReconciliationStatus,
    ) -> Result<(Sequence, agentmod_primitives::EventId), RunTurnError> {
        self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ProcessReconciliationCompleted(
                ProcessReconciliationCompletedEvent {
                    call_id: call_id.into(),
                    process_id: process_id.into(),
                    status,
                },
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_tool_failure(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        position: JournalPosition,
        call_id: &str,
        code: &str,
        message: &str,
        retryable: bool,
    ) -> Result<(Sequence, agentmod_primitives::EventId), RunTurnError> {
        self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ToolExecutionFailed(ToolExecutionFailedEvent {
                call_id: call_id.into(),
                code: code.into(),
                message: message.into(),
                retryable,
            }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_tool_conversation(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut position: JournalPosition,
        call_id: &str,
        action: &crate::action::ToolCallAction,
        result: &Value,
        artifact: Option<&str>,
        truncated: bool,
    ) -> Result<JournalPosition, RunTurnError> {
        let call_sequence = position
            .sequence
            .checked_next()
            .map_err(|_| RunTurnError::SequenceOverflow)?;
        (position.sequence, position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
                entry: ConversationEntry::ToolCallRequest(ToolCallEntry {
                    id: ConversationEntryId(format!("tool-call:{call_id}:{}", call_sequence.get())),
                    call_id: call_id.into(),
                    tool: action.tool.clone(),
                    arguments: action.arguments.clone(),
                    source_sequence: call_sequence,
                }),
            }),
        )?;
        let result_sequence = position
            .sequence
            .checked_next()
            .map_err(|_| RunTurnError::SequenceOverflow)?;
        let mut content =
            serde_json::to_string(result).map_err(|_| RunTurnError::ToolResultEncoding)?;
        let projection_truncated = content.len() > MAX_TOOL_RESULT_BYTES;
        if projection_truncated {
            content.truncate(MAX_TOOL_RESULT_BYTES);
        }
        let artifact_id = artifact
            .map(str::parse)
            .transpose()
            .map_err(|_| RunTurnError::InvalidArtifact)?;
        (position.sequence, position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
                entry: ConversationEntry::ToolResult(ToolResultEntry {
                    id: ConversationEntryId(format!(
                        "tool-result:{call_id}:{}",
                        result_sequence.get()
                    )),
                    call_id: call_id.into(),
                    content,
                    artifact_id,
                    truncated: truncated || projection_truncated,
                    source_sequence: result_sequence,
                }),
            }),
        )?;
        Ok(position)
    }

    fn commit_user(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        command: &RunTurnCommand,
    ) -> Result<
        (
            crate::session::SessionState,
            Sequence,
            EventEnvelope<RuntimeCommittedEvent>,
        ),
        RunTurnError,
    > {
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        let sequence = loaded
            .state
            .last_sequence
            .checked_next()
            .map_err(|_| RunTurnError::SequenceOverflow)?;
        let event = self.seal_event(
            session_id,
            sequence,
            None,
            RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
                entry: ConversationEntry::UserMessage(TextEntry {
                    id: ConversationEntryId(format!(
                        "user:{}:{}",
                        sequence.get(),
                        command.cancellation_id
                    )),
                    text: command.prompt.clone(),
                    source_sequence: sequence,
                }),
            }),
        )?;
        persistence
            .commit_event(CommitSessionEventCommand {
                session_directory: session_directory.to_owned(),
                event: event.clone(),
                durability: CommitDurability::Data,
            })
            .map_err(RunTurnError::Persistence)?;
        let state = reduce(Some(loaded.state), &event).map_err(RunTurnError::Reducer)?;
        Ok((state, sequence, event))
    }

    fn commit_scheduler_fired(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        scheduled: ScheduledTurnPrelude,
    ) -> Result<(), RunTurnError> {
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        self.commit_next(
            persistence,
            session_id,
            session_directory,
            loaded.state.last_sequence,
            loaded.last_event_id,
            RuntimeCommittedEvent::SchedulerFired(SchedulerFiredEvent {
                execution_id: scheduled.execution_id,
                schedule_id: scheduled.schedule_id,
                scheduled_for_ms: scheduled.scheduled_for_ms,
            }),
        )?;
        Ok(())
    }

    fn commit_next(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        previous_sequence: Sequence,
        previous_event_id: agentmod_primitives::EventId,
        payload: RuntimeCommittedEvent,
    ) -> Result<(Sequence, agentmod_primitives::EventId), RunTurnError> {
        let sequence = previous_sequence
            .checked_next()
            .map_err(|_| RunTurnError::SequenceOverflow)?;
        let event = self.seal_event(
            session_id,
            sequence,
            Some(CausationId::from_uuid(previous_event_id.into_uuid())),
            payload,
        )?;
        let event_id = event.metadata.event_id;
        persistence
            .commit_event(CommitSessionEventCommand {
                session_directory: session_directory.to_owned(),
                event,
                durability: CommitDurability::Data,
            })
            .map_err(RunTurnError::Persistence)?;
        Ok((sequence, event_id))
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_provider_events(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut sequence: Sequence,
        mut event_id: agentmod_primitives::EventId,
        cancellation_id: &str,
        events: &[ProviderEvent],
    ) -> Result<(Sequence, agentmod_primitives::EventId), RunTurnError> {
        for provider_event in events {
            let payload = match provider_event {
                ProviderEvent::Started => {
                    RuntimeCommittedEvent::ModelRequestStarted(ModelRequestStartedEvent {
                        cancellation_id: cancellation_id.to_owned(),
                    })
                }
                ProviderEvent::Text(text) => {
                    RuntimeCommittedEvent::ModelOutputDeltaObserved(ModelOutputDeltaObservedEvent {
                        cancellation_id: cancellation_id.to_owned(),
                        text: text.clone(),
                    })
                }
                ProviderEvent::ToolDelta {
                    call_id,
                    name,
                    arguments,
                } => RuntimeCommittedEvent::ModelToolCallDeltaObserved(
                    ModelToolCallDeltaObservedEvent {
                        call_id: call_id.clone(),
                        name: name.clone(),
                        arguments: arguments.clone(),
                    },
                ),
                ProviderEvent::ToolProposed {
                    continuation_id,
                    call_id,
                    tool,
                    arguments,
                } => RuntimeCommittedEvent::ModelToolCallProposed(ModelToolCallProposedEvent {
                    continuation_id: continuation_id.clone(),
                    call_id: call_id.clone(),
                    tool: tool.clone(),
                    arguments: arguments.clone(),
                }),
                ProviderEvent::Completed {
                    reason,
                    input_tokens,
                    output_tokens,
                } => RuntimeCommittedEvent::ModelResponseCompleted(ModelResponseCompletedEvent {
                    cancellation_id: cancellation_id.to_owned(),
                    finish_reason: reason.clone(),
                    input_tokens: *input_tokens,
                    output_tokens: *output_tokens,
                }),
                ProviderEvent::Cancelled => {
                    RuntimeCommittedEvent::ModelRequestCancelled(ModelRequestCancelledEvent {
                        cancellation_id: cancellation_id.to_owned(),
                    })
                }
                ProviderEvent::Failed {
                    code,
                    message,
                    retryable,
                } => RuntimeCommittedEvent::ModelRequestFailed(ModelRequestFailedEvent {
                    code: code.clone(),
                    message: message.clone(),
                    retryable: *retryable,
                }),
            };
            (sequence, event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                sequence,
                event_id,
                payload,
            )?;
        }
        Ok((sequence, event_id))
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_visible_assistant(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: PathBuf,
        previous_sequence: Sequence,
        previous_event_id: agentmod_primitives::EventId,
        cancellation_id: &str,
        events: &[ProviderEvent],
    ) -> Result<Sequence, RunTurnError> {
        let visible_text: String = events
            .iter()
            .filter_map(|event| match event {
                ProviderEvent::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect();
        if visible_text.is_empty() {
            return Ok(previous_sequence);
        }
        let sequence = previous_sequence
            .checked_next()
            .map_err(|_| RunTurnError::SequenceOverflow)?;
        let event = self.seal_event(
            session_id,
            sequence,
            Some(CausationId::from_uuid(previous_event_id.into_uuid())),
            RuntimeCommittedEvent::ConversationEntryCommitted(ConversationEntryCommittedEvent {
                entry: ConversationEntry::AssistantMessage(TextEntry {
                    id: ConversationEntryId(format!(
                        "assistant:{}:{cancellation_id}",
                        sequence.get()
                    )),
                    text: visible_text,
                    source_sequence: sequence,
                }),
            }),
        )?;
        persistence
            .commit_event(CommitSessionEventCommand {
                session_directory,
                event,
                durability: CommitDurability::Data,
            })
            .map_err(RunTurnError::Persistence)?;
        Ok(sequence)
    }

    fn seal_event(
        &self,
        session_id: SessionId,
        sequence: Sequence,
        causation_id: Option<CausationId>,
        payload: RuntimeCommittedEvent,
    ) -> Result<EventEnvelope<RuntimeCommittedEvent>, RunTurnError> {
        let identity = self
            .data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(RunTurnError::Identity)?;
        let event_type = payload.event_type().to_owned();
        EventEnvelope::seal(
            EventMetadata {
                event_id: identity.event_id,
                scope: EventScope::Session(session_id),
                sequence,
                timestamp: identity.timestamp,
                event_type,
                event_version: Version::new(1, 0),
                correlation_id: identity.correlation_id,
                causation_id: causation_id.unwrap_or(identity.causation_id),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: "runtime".into(),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            payload,
        )
        .map_err(|_| RunTurnError::Event)
    }
}

fn validate(command: &RunTurnCommand) -> Result<(), RunTurnError> {
    if command.sessions_root.as_os_str().is_empty()
        || command.session_id.trim().is_empty()
        || command.prompt.trim().is_empty()
        || command.prompt.len() > MAX_PROMPT_BYTES
        || command.provider.trim().is_empty()
        || command.model.trim().is_empty()
        || command.cancellation_id.trim().is_empty()
        || !command.options.is_object()
    {
        return Err(RunTurnError::Invalid);
    }
    Ok(())
}

const fn approval_recovery_action(
    transitioned: bool,
    disposition: ApprovalDisposition,
    approval_state: ApprovalState,
    execution_state: Option<ToolExecutionState>,
) -> ApprovalRecoveryAction {
    if transitioned {
        return ApprovalRecoveryAction::CommitAndResume;
    }
    match (disposition, approval_state, execution_state) {
        (ApprovalDisposition::Denied, _, _)
        | (
            ApprovalDisposition::Approved,
            ApprovalState::Approved,
            Some(ToolExecutionState::Terminal),
        ) => ApprovalRecoveryAction::Idempotent,
        (ApprovalDisposition::Approved, ApprovalState::Pending, _) => {
            ApprovalRecoveryAction::CommitAndResume
        }
        (ApprovalDisposition::Approved, ApprovalState::Approved, None) => {
            ApprovalRecoveryAction::Resume
        }
        (
            ApprovalDisposition::Approved,
            ApprovalState::Approved,
            Some(ToolExecutionState::Dispatched | ToolExecutionState::Started),
        ) => ApprovalRecoveryAction::Reconcile,
        (ApprovalDisposition::Approved, ApprovalState::Denied, _) => {
            ApprovalRecoveryAction::Invalid
        }
    }
}

fn invalid_replacement() -> RunTurnError {
    RunTurnError::Provider(ProviderExecutionError::InvalidInterceptionReplacement)
}

fn project(entries: &[ConversationEntry]) -> Vec<ProviderEntry> {
    entries
        .iter()
        .map(|entry| match entry {
            ConversationEntry::SystemInstruction(value)
            | ConversationEntry::ProjectInstruction(value)
            | ConversationEntry::UserInstruction(value) => {
                ProviderEntry::System(value.text.clone())
            }
            ConversationEntry::UserMessage(value) => ProviderEntry::User(value.text.clone()),
            ConversationEntry::AssistantMessage(value) => {
                ProviderEntry::Assistant(value.text.clone())
            }
            ConversationEntry::ToolCallRequest(value) => ProviderEntry::ToolCall {
                call_id: value.call_id.clone(),
                tool: value.tool.clone(),
                arguments: value.arguments.clone(),
            },
            ConversationEntry::ToolResult(value) => ProviderEntry::ToolResult {
                call_id: value.call_id.clone(),
                content: value.content.clone(),
                truncated: value.truncated,
            },
            ConversationEntry::ContextSummary(value) => ProviderEntry::Summary {
                text: value.text.clone(),
                start: value.source_start.get(),
                end: value.source_end.get(),
            },
            ConversationEntry::ProviderVisibleMetadata(value) => ProviderEntry::Metadata {
                key: value.key.clone(),
                value: value.value.clone(),
            },
            ConversationEntry::RetrievedMemory(value) => ProviderEntry::Metadata {
                key: format!("memory:{}", value.provider),
                value: json!({
                    "scope": value.scope,
                    "source": value.source,
                    "score": value.score,
                    "content": value.content
                }),
            },
            ConversationEntry::RuntimeAnnotation(value) => ProviderEntry::Metadata {
                key: "runtime_annotation".into(),
                value: Value::String(value.text.clone()),
            },
            ConversationEntry::Attachment(value)
            | ConversationEntry::Image(value)
            | ConversationEntry::ArtifactReference(value) => ProviderEntry::Metadata {
                key: "artifact".into(),
                value: json!({
                    "id": value.artifact_id,
                    "hash": value.content_hash,
                    "mime_type": value.mime_type,
                    "label": value.label
                }),
            },
            ConversationEntry::PendingTask(value) => ProviderEntry::Metadata {
                key: "pending_task".into(),
                value: json!({
                    "id": value.task_id,
                    "description": value.description,
                    "state": value.state
                }),
            },
            ConversationEntry::ActiveProcessSummary(value) => ProviderEntry::Metadata {
                key: "active_process".into(),
                value: json!({
                    "id": value.process_id,
                    "label": value.label,
                    "state": value.state
                }),
            },
            ConversationEntry::ChildAgentHandoff(value) => ProviderEntry::Metadata {
                key: "child_agent_handoff".into(),
                value: json!({
                    "session": value.child_session,
                    "summary": value.summary,
                    "artifact_id": value.artifact_id
                }),
            },
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum RunTurnError {
    #[error("turn request is invalid")]
    Invalid,
    #[error("session identifier is invalid")]
    InvalidSession,
    #[error("continuation identifier is invalid")]
    InvalidContinuation,
    #[error("continuation payload is invalid")]
    InvalidContinuationPayload,
    #[error("tool execution `{0}` may have crossed the external side-effect boundary")]
    AmbiguousToolExecution(String),
    #[error("continuation operation failed: {0}")]
    Continuation(crate::continuation::ContinuationLogicError),
    #[error("event sequence overflow")]
    SequenceOverflow,
    #[error("event identity failed: {0}")]
    Identity(EventIdentityDataError),
    #[error("canonical event could not be sealed")]
    Event,
    #[error("session persistence failed: {0}")]
    Persistence(SessionPersistenceLogicError),
    #[error("session replay failed: {0}")]
    Reducer(SessionReducerError),
    #[error("provider execution failed: {0}")]
    Provider(ProviderExecutionError),
    #[error("tool execution failed: {0}")]
    Tool(ToolExecutionError),
    #[error("tool loop exceeded the configured step limit")]
    ToolStepLimit,
    #[error("tool result could not be encoded")]
    ToolResultEncoding,
    #[error("tool host returned an invalid artifact identifier")]
    InvalidArtifact,
    #[error("durable tool receipt does not match dispatch {0}")]
    InvalidRecoveryReceipt(String),
    #[error(
        "provider execution failed ({provider}) and its audit event could not be committed ({audit})"
    )]
    ProviderFailureAudit { provider: String, audit: String },
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    };

    use agentmod_event_model::{EventClassification, EventMetadata, EventOrigin, EventScope};
    use agentmod_event_pipeline::BlockingPipelineBuilder;
    use agentmod_primitives::{
        ByteCount, ContentHash, CorrelationId, EventId, TimestampMillis, Version,
    };
    use agentmod_runtime_data::{
        continuation::{
            ContinuationDataError, ContinuationDataPort, ContinuationPayloadRecord,
            ContinuationRecord, ContinuationStateRecord, CreateContinuationDataRequest,
            ResolveContinuationDataRecord, ResolveContinuationDataRequest,
        },
        harness::{
            HarnessDataCommand, HarnessDataError, HarnessDataEvent, HarnessDataPort,
            HarnessDataReply,
        },
        identity::{
            AllocateEventIdentityDataRequest, EventIdentityDataError, EventIdentityDataPort,
            EventIdentityDataRecord,
        },
        journal::{
            AppendEventDataRequest, AppendedEventDataRecord, JournalDataError,
            JournalEventDataPort, JournalEventDataRecord, JournalRecoveryStatus,
            RecoverJournalDataRequest, RecoveredJournalDataRecord, ScanEventsDataRequest,
            ScannedEventsDataRecord,
        },
        tool::{ExecuteToolDataRequest, ToolDataError, ToolDataEvent, ToolDataPort},
    };
    use serde_json::Value;
    use uuid::Uuid;

    use crate::{
        permission::{PermissionEffect, PermissionPolicy},
        session::{RuntimeCommittedEvent, SessionCreatedEvent},
    };

    use super::*;

    #[derive(Clone)]
    struct MockTurnData {
        state: Arc<MockTurnState>,
    }

    struct MockTurnState {
        events: StdMutex<Vec<EventEnvelope<Value>>>,
        harness_commands: StdMutex<Vec<HarnessDataCommand>>,
        harness_reply: StdMutex<Result<HarnessDataReply, HarnessDataError>>,
        next_identity: AtomicU64,
    }

    impl MockTurnData {
        fn new(reply: Result<HarnessDataReply, HarnessDataError>) -> Self {
            Self {
                state: Arc::new(MockTurnState {
                    events: StdMutex::new(vec![created_event()]),
                    harness_commands: StdMutex::new(Vec::new()),
                    harness_reply: StdMutex::new(reply),
                    next_identity: AtomicU64::new(100),
                }),
            }
        }

        fn event_types(&self) -> Vec<String> {
            self.state
                .events
                .lock()
                .expect("events")
                .iter()
                .map(|event| event.metadata.event_type.clone())
                .collect()
        }
    }

    impl JournalEventDataPort for MockTurnData {
        fn append_event(
            &self,
            request: AppendEventDataRequest,
        ) -> Result<AppendedEventDataRecord, JournalDataError> {
            let mut events = self.state.events.lock().expect("events");
            assert_eq!(
                request.event.metadata.sequence.get(),
                events.len() as u64 + 1
            );
            let sequence = request.event.metadata.sequence;
            let event_id = request.event.metadata.event_id;
            let envelope_checksum = request.event.integrity_checksum;
            events.push(request.event);
            Ok(AppendedEventDataRecord {
                event_id,
                sequence,
                envelope_checksum,
                journal_checksum: ContentHash::digest(
                    format!("journal-{}", sequence.get()).as_bytes(),
                ),
                offset: ByteCount::new((sequence.get() - 1) * 100),
                journal_bytes: ByteCount::new(sequence.get() * 100),
            })
        }

        fn scan_events(
            &self,
            _request: ScanEventsDataRequest,
        ) -> Result<ScannedEventsDataRecord, JournalDataError> {
            let events = self.state.events.lock().expect("events").clone();
            let mut previous = None;
            let records = events
                .into_iter()
                .map(|event| {
                    let checksum = ContentHash::digest(
                        format!("journal-{}", event.metadata.sequence.get()).as_bytes(),
                    );
                    let record = JournalEventDataRecord {
                        offset: ByteCount::new((event.metadata.sequence.get() - 1) * 100),
                        event,
                        journal_checksum: checksum,
                        previous_journal_checksum: previous,
                    };
                    previous = Some(checksum);
                    record
                })
                .collect();
            Ok(ScannedEventsDataRecord {
                events: records,
                valid_bytes: ByteCount::new(
                    self.state.events.lock().expect("events").len() as u64 * 100,
                ),
            })
        }

        fn recover_journal(
            &self,
            _request: RecoverJournalDataRequest,
        ) -> Result<RecoveredJournalDataRecord, JournalDataError> {
            Ok(RecoveredJournalDataRecord {
                status: JournalRecoveryStatus::Clean,
                valid_bytes: ByteCount::new(0),
            })
        }
    }

    impl EventIdentityDataPort for MockTurnData {
        fn allocate_event_identity(
            &self,
            _request: AllocateEventIdentityDataRequest,
        ) -> Result<EventIdentityDataRecord, EventIdentityDataError> {
            let value = self.state.next_identity.fetch_add(1, Ordering::Relaxed);
            Ok(EventIdentityDataRecord {
                event_id: EventId::from_uuid(Uuid::from_u128(u128::from(value))),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(u128::from(
                    value + 1_000,
                ))),
                causation_id: CausationId::from_uuid(Uuid::from_u128(u128::from(value + 2_000))),
                timestamp: TimestampMillis::new(i64::try_from(value).expect("timestamp")),
            })
        }
    }

    impl ContinuationDataPort for MockTurnData {
        fn create(
            &self,
            _request: CreateContinuationDataRequest,
        ) -> Result<(), ContinuationDataError> {
            Ok(())
        }

        fn load(
            &self,
            _session_id: &str,
            _id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            unreachable!("fixture does not load continuations")
        }

        fn resolve(
            &self,
            _request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            Ok(ResolveContinuationDataRecord {
                transitioned: false,
                state: ContinuationStateRecord::Cancelled,
                payload: ContinuationPayloadRecord::Opaque {
                    label: "fixture".into(),
                },
            })
        }
    }

    #[async_trait]
    impl ToolDataPort for MockTurnData {
        async fn execute_tool(
            &self,
            _request: ExecuteToolDataRequest,
        ) -> Result<Vec<ToolDataEvent>, ToolDataError> {
            Err(ToolDataError::Unavailable)
        }
    }

    #[async_trait]
    impl HarnessDataPort for MockTurnData {
        async fn exchange(
            &self,
            command: HarnessDataCommand,
        ) -> Result<HarnessDataReply, HarnessDataError> {
            self.state
                .harness_commands
                .lock()
                .expect("commands")
                .push(command);
            self.state.harness_reply.lock().expect("reply").clone()
        }

        async fn exchange_events(
            &self,
            command: HarnessDataCommand,
        ) -> Result<agentmod_runtime_data::harness::HarnessDataEventStream, HarnessDataError>
        {
            let HarnessDataReply::Events(events) = self.exchange(command).await? else {
                return Err(HarnessDataError::Unavailable);
            };
            Ok(agentmod_runtime_data::harness::HarnessDataEventStream::from_events(events))
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(1))
    }

    fn created_event() -> EventEnvelope<Value> {
        let payload = RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
            workspace: "fixture-workspace".into(),
            style: "persistent-chat".into(),
        });
        let typed = EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(2)),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::FIRST,
                timestamp: TimestampMillis::new(1),
                event_type: payload.event_type().into(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(3)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(4)),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: "runtime".into(),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            payload,
        )
        .expect("typed event");
        EventEnvelope::seal(
            typed.metadata,
            serde_json::to_value(typed.payload).expect("payload"),
        )
        .expect("data event")
    }

    fn policy(effect: PermissionEffect) -> ProviderExecutionPolicy {
        let pipeline = || {
            Arc::new(
                BlockingPipelineBuilder::<crate::action::ActionProposal>::new()
                    .compile()
                    .expect("pipeline"),
            )
        };
        ProviderExecutionPolicy {
            style_pipeline: pipeline(),
            plugin_pipeline: pipeline(),
            user_policy: PermissionPolicy::new("user", vec![], effect, "user default"),
            mandatory_policy: PermissionPolicy::new(
                "mandatory",
                vec![],
                PermissionEffect::Allow,
                "mandatory allow",
            ),
        }
    }

    fn command() -> RunTurnCommand {
        RunTurnCommand {
            sessions_root: PathBuf::from("sessions"),
            session_id: session_id().to_string(),
            prompt: "inspect the repository".into(),
            provider: "deterministic-mock".into(),
            model: "fixture".into(),
            options: serde_json::json!({}),
            cancellation_id: "cancel-1".into(),
        }
    }

    #[test]
    fn approval_recovery_uses_receipts_for_a_dispatched_action() {
        assert_eq!(
            approval_recovery_action(
                false,
                ApprovalDisposition::Approved,
                ApprovalState::Pending,
                None,
            ),
            ApprovalRecoveryAction::CommitAndResume
        );
        assert_eq!(
            approval_recovery_action(
                false,
                ApprovalDisposition::Approved,
                ApprovalState::Approved,
                None,
            ),
            ApprovalRecoveryAction::Resume
        );
        assert_eq!(
            approval_recovery_action(
                false,
                ApprovalDisposition::Approved,
                ApprovalState::Approved,
                Some(ToolExecutionState::Dispatched),
            ),
            ApprovalRecoveryAction::Reconcile
        );
        assert_eq!(
            approval_recovery_action(
                false,
                ApprovalDisposition::Approved,
                ApprovalState::Approved,
                Some(ToolExecutionState::Terminal),
            ),
            ApprovalRecoveryAction::Idempotent
        );
    }

    #[test]
    fn process_reconciliation_classification_is_bounded_and_explicit() {
        assert_eq!(
            process_reconciliation_status(&json!({"recovery_status":"live"})),
            ProcessReconciliationStatus::Live
        );
        assert_eq!(
            process_reconciliation_status(
                &json!({"recovery_status":"recovered_running_unattached"})
            ),
            ProcessReconciliationStatus::RecoveredRunningUnattached
        );
        assert_eq!(
            process_reconciliation_status(&json!({"recovery_status":"dispatch_uncertain"})),
            ProcessReconciliationStatus::DispatchUncertain
        );
        assert_eq!(
            process_reconciliation_status(&json!({"recovery_status":"unexpected"})),
            ProcessReconciliationStatus::Failed
        );
    }

    #[tokio::test]
    async fn successful_turn_maps_data_and_commits_lifecycle_in_order() {
        let data = MockTurnData::new(Ok(HarnessDataReply::Events(vec![
            HarnessDataEvent::Started,
            HarnessDataEvent::Text("done".into()),
            HarnessDataEvent::Completed {
                reason: "stop".into(),
                input_tokens: 4,
                output_tokens: 1,
            },
        ])));
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let result = logic.run_turn(command()).await.expect("turn");

        assert_eq!(result.first_committed_sequence.get(), 2);
        assert_eq!(result.last_committed_sequence.get(), 8);
        assert_eq!(
            data.event_types(),
            vec![
                "session.created",
                "conversation.entry_committed",
                "model.request_proposed",
                "model.request_approved",
                "model.request_started",
                "model.output_delta_observed",
                "model.response_completed",
                "conversation.entry_committed",
            ]
        );
        let commands = data.state.harness_commands.lock().expect("commands");
        let HarnessDataCommand::Execute {
            provider,
            model,
            entries,
            grant,
            ..
        } = &commands[0]
        else {
            panic!("execute command");
        };
        assert_eq!(provider, "deterministic-mock");
        assert_eq!(model, "fixture");
        assert_eq!(grant.len(), 64);
        assert!(matches!(
            entries.last(),
            Some(agentmod_runtime_data::harness::HarnessDataEntry::User(value))
                if value == "inspect the repository"
        ));
    }

    #[tokio::test]
    async fn turn_stream_emits_only_after_each_provider_event_is_committed() {
        let data = MockTurnData::new(Ok(HarnessDataReply::Events(vec![
            HarnessDataEvent::Started,
            HarnessDataEvent::Text("done".into()),
            HarnessDataEvent::Completed {
                reason: "stop".into(),
                input_tokens: 4,
                output_tokens: 1,
            },
        ])));
        let logic = TurnLogic::new(data, policy(PermissionEffect::Allow));
        let mut stream = logic
            .run_turn_stream(command())
            .await
            .expect("start stream");
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.expect("stream item"));
        }
        assert!(matches!(
            items.as_slice(),
            [
                RunTurnStreamItem::Event {
                    event: ProviderEvent::Started,
                    committed_sequence: started,
                },
                RunTurnStreamItem::Event {
                    event: ProviderEvent::Text(text),
                    committed_sequence: text_sequence,
                },
                RunTurnStreamItem::Event {
                    event: ProviderEvent::Completed { .. },
                    committed_sequence: completed,
                },
                RunTurnStreamItem::Complete {
                    first_committed_sequence: first,
                    last_committed_sequence: last,
                    awaiting_continuation: None,
                },
            ] if started.get() == 5
                && text == "done"
                && text_sequence.get() == 6
                && completed.get() == 7
                && first.get() == 2
                && last.get() == 8
        ));
    }

    #[tokio::test]
    async fn denial_records_original_proposal_before_authorization_failure() {
        let data = MockTurnData::new(Ok(HarnessDataReply::Events(vec![])));
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Deny));

        let error = logic.run_turn(command()).await.expect_err("denied");

        assert!(matches!(
            error,
            RunTurnError::Provider(ProviderExecutionError::Rejected(_))
        ));
        assert_eq!(
            data.event_types(),
            vec![
                "session.created",
                "conversation.entry_committed",
                "model.request_proposed",
                "model.request_failed",
            ]
        );
        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn harness_failure_is_committed_after_approval() {
        let data = MockTurnData::new(Err(HarnessDataError::Unavailable));
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic
            .run_turn(command())
            .await
            .expect_err("harness failure");

        assert!(matches!(
            error,
            RunTurnError::Provider(ProviderExecutionError::Unavailable)
        ));
        assert_eq!(
            data.event_types(),
            vec![
                "session.created",
                "conversation.entry_committed",
                "model.request_proposed",
                "model.request_approved",
                "model.request_failed",
            ]
        );
    }
}
