//! Durable runtime-owned session turn coordination.
#![allow(
    missing_docs,
    reason = "logic-local turn records are intentionally boundary-specific"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Weak},
};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_event_pipeline::ActionCapabilities;
use agentmod_primitives::{
    CausationId, ContentHash, ContinuationId, Sequence, SessionId, TimestampMillis, Version,
};
use agentmod_runtime_data::{
    artifact::ArtifactDataPort,
    continuation::ContinuationDataPort,
    harness::HarnessDataPort,
    identity::{
        AllocateEventIdentityDataRequest, EventIdentityDataError, EventIdentityDataPort,
        EventIdentityDataRecord,
    },
    journal::JournalEventDataPort,
    memory::MemoryDataPort,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};

use crate::{
    action::{ActionProposal, ConsequentialAction, ProposalId},
    artifact::{
        ArtifactPersistenceLogic, ArtifactRetention, ArtifactSecurity, PersistArtifactCommand,
    },
    child_session::{ChildSessionLogicPort, EnsureChildSessionCommand},
    compaction::{CompactionContext, CompactionError, CompactionStrategy, compact_projection},
    continuation::{
        ApprovalDisposition, ContinuationLogic, ContinuationLogicPort, ContinuationPayload,
        ContinuationState, ContinuationWakeCondition, ContinuationWakeProof,
        CreateContinuationCommand, DeferredTurnContinuation, LoadContinuationQuery,
        PendingToolCallContinuation, ResolveApprovalCommand, StyleApprovalContinuation,
        ToolApprovalContinuation, WakeContinuationCommand,
    },
    conversation::{
        ChildHandoffEntry, ConversationEntry, ConversationEntryId, PendingTaskEntry,
        ProjectionProvenance, RetrievedMemoryEntry, TextEntry, ToolCallEntry, ToolResultEntry,
    },
    harness::{
        AuthorizedProviderRequest, ExecuteProviderCommand, ProviderEvent, ProviderEventStream,
        ProviderExecutionError, ProviderExecutionLogic, ProviderExecutionPolicy,
        ProviderExecutionPort,
    },
    interception::{
        InterceptionOutcome, InterceptorAuditResult, InterceptorAuditStep, InterceptorScope,
        intercept_action,
    },
    memory::{MemoryLogic, MemoryLogicError, MemoryLogicPort, MemoryScope, RetrieveMemoryCommand},
    persistence::{
        CommitDurability, CommitSessionEventCommand, LoadSessionCommand, LoadSessionResult,
        SessionPersistenceLogic, SessionPersistenceLogicError, SessionPersistenceLogicPort,
    },
    plugin::{
        CommittedPluginEvent, ComposePluginPipelineCommand, ObserveCommittedPluginEventsCommand,
        PluginCompositionError, PluginCompositionLogicPort, PluginObservationSummary,
    },
    projection::{
        ProjectionMeasure, ProjectionMeasureError, canonical_json_bytes, measure_projection,
        project,
    },
    session::{
        ApprovalRequestedEvent, ApprovalResolvedEvent, ApprovalState,
        ArtifactPersistenceApprovedEvent, ArtifactPersistenceCompletedEvent,
        ArtifactPersistenceDispatchedEvent, ArtifactPersistenceIdentity,
        ArtifactPersistenceProposedEvent, ArtifactPersistenceResumeAction,
        ChildAgentCompletedEvent, ChildAgentCreatedEvent, ChildAgentCreationApprovedEvent,
        ChildAgentCreationProposedEvent, ChildAgentExecutionIdentity, ChildAgentState,
        ChildJoinCompletedEvent, ContextBoundaryCompletedEvent, ContextBoundaryIdentity,
        ContextBoundaryOrigin, ContextBoundaryStartedEvent, ContextPhaseCompletedEvent,
        ContextPhaseIdentity, ContextPhaseStartedEvent, ContextProjectionReplacedEvent,
        ConversationEntryCommittedEvent, ModelOutputDeltaObservedEvent, ModelRequestApprovedEvent,
        ModelRequestCancelledEvent, ModelRequestFailedEvent, ModelRequestProposedEvent,
        ModelRequestStartedEvent, ModelResponseCompletedEvent, ModelToolCallDeltaObservedEvent,
        ModelToolCallProposedEvent, PlannedTask, PluginInvocationCompletedEvent,
        PluginSetActivatedEvent, ProcessReconciliationCompletedEvent,
        ProcessReconciliationStartedEvent, ProcessReconciliationStatus,
        ReviewerFindingsCommittedEvent, RuntimeCommittedEvent, SchedulerDeliveryReconciledEvent,
        SchedulerFiredEvent, SessionReducerError, StyleExecutionControlState,
        StyleExecutionInitializedEvent, StyleExecutionTerminatedEvent, StyleNodeCompletedEvent,
        StyleNodeEnteredEvent, StyleNodeFailedEvent, StyleTransitionSelectedEvent,
        TaskPlanCommittedEvent, ToolCallApprovedEvent, ToolCallProposedEvent,
        ToolExecutionCompletedEvent, ToolExecutionDispatchedEvent, ToolExecutionFailedEvent,
        ToolExecutionStartedEvent, ToolExecutionState, ToolExecutionTerminalOutcome,
        ToolOutputObservedEvent, reduce,
    },
    style_executor::{
        CompiledStyleExecutor, StyleAdapterKind, StyleExecutorError, StyleNodeCursor,
        StyleNodeDirective,
    },
    tool::{
        AuthorizedToolRequest, PrepareToolCommand, ToolAuthorizationOutcome, ToolEvent,
        ToolExecutionError, ToolExecutionLogic, ToolExecutionPolicy, ToolOutputStream,
    },
};

const MAX_PROMPT_BYTES: usize = 1024 * 1024;
const MAX_TOOL_STEPS: usize = 16;
const MAX_TOOL_RESULT_BYTES: usize = 64 * 1024;
const MAX_MEMORY_QUERY_BYTES: usize = 1024 * 1024;
const RUNTIME_PLUGIN_API_VERSION: &str = "0.1.0";
const DEFAULT_SLIDING_WINDOW_ENTRIES: usize = 32;
const MAX_PROVIDER_PROJECTION_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContextCompositionBoundary {
    TurnStart,
    BeforeModelRequest,
    BeforeTurnCompletion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContextCompositionOrigin {
    UserTurn,
    ChildTask,
    ToolContinuation,
    ApprovalContinuation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TurnInputOrigin {
    User,
    ChildTask,
}

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

struct SessionPluginPolicy {
    execution: ProviderExecutionPolicy,
    activated_plugin_ids: Vec<String>,
}

struct ActiveStyleTurn {
    executor: CompiledStyleExecutor,
    current: StyleNodeCursor,
    position: JournalPosition,
    attempt: u32,
    loop_iteration: u32,
    step: u64,
    max_steps: u64,
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
pub struct ObserveCommittedEventsCommand {
    pub sessions_root: PathBuf,
    pub session_id: String,
    pub events: Vec<CommittedPluginEvent>,
}

#[async_trait]
pub trait CommittedEventObserverLogicPort: Send + Sync {
    async fn observe_committed_events(
        &self,
        command: ObserveCommittedEventsCommand,
    ) -> Result<PluginObservationSummary, RunTurnError>;
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
    pub allow_resumed_recovery: bool,
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
pub struct RecordScheduledRecoveryCommand {
    pub sessions_root: PathBuf,
    pub session_id: String,
    pub execution_id: String,
    pub schedule_id: String,
    pub outcome: String,
    pub continuation_id: Option<String>,
}

pub trait ScheduledRecoveryLogicPort: Send + Sync {
    /// Commits one canonical scheduler reconciliation outcome.
    ///
    /// # Errors
    ///
    /// Returns [`RunTurnError`] when identity, history, or persistence is invalid.
    fn record_scheduled_recovery(
        &self,
        command: RecordScheduledRecoveryCommand,
    ) -> Result<Sequence, RunTurnError>;
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
    artifacts: ArtifactPersistenceLogic<D>,
    policy: ProviderExecutionPolicy,
    session_gates: Arc<Mutex<BTreeMap<String, Weak<Mutex<()>>>>>,
    child_sessions: Option<Arc<dyn ChildSessionLogicPort>>,
    plugins: Option<Arc<dyn PluginCompositionLogicPort>>,
}

impl<D> TurnLogic<D> {
    #[must_use]
    pub fn with_plugins(mut self, plugins: Arc<dyn PluginCompositionLogicPort>) -> Self {
        self.plugins = Some(plugins);
        self
    }

    async fn policy_for_state(
        &self,
        state: &crate::session::SessionState,
        cancellation_id: &str,
    ) -> Result<SessionPluginPolicy, RunTurnError> {
        let Some(binding) = state.style_binding.as_ref() else {
            if self.plugins.is_none() {
                return Ok(SessionPluginPolicy {
                    execution: self.policy.clone(),
                    activated_plugin_ids: Vec::new(),
                });
            }
            return Err(RunTurnError::StyleMigrationRequired);
        };
        let compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&binding.compiled_style_json)
                .map_err(|_| RunTurnError::StyleBindingInvalid)?;
        let has_external_interceptor = compiled.interceptors.iter().any(|declaration| {
            declaration.owner != "runtime" && !declaration.owner.starts_with("runtime.")
        });
        let has_external_plugin = compiled
            .allowed_plugins
            .iter()
            .any(|plugin| plugin != "runtime" && !plugin.starts_with("runtime."));
        if !has_external_interceptor && !has_external_plugin {
            return Ok(SessionPluginPolicy {
                execution: self.policy.clone(),
                activated_plugin_ids: Vec::new(),
            });
        }
        let plugins = self
            .plugins
            .as_ref()
            .ok_or(RunTurnError::PluginCompositionUnavailable)?;
        let composed = plugins
            .compose_pipeline(ComposePluginPipelineCommand {
                session_id: state.id.to_string(),
                cancellation_id: cancellation_id.to_owned(),
                compiled_style: compiled,
                runtime_api_version: String::from(RUNTIME_PLUGIN_API_VERSION),
            })
            .await
            .map_err(RunTurnError::PluginComposition)?;
        let mut policy = self.policy.clone();
        policy.plugin_pipeline = composed.pipeline;
        Ok(SessionPluginPolicy {
            execution: policy,
            activated_plugin_ids: composed.activated_plugin_ids,
        })
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
        + MemoryDataPort
        + ArtifactDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
{
    #[must_use]
    pub fn new(data: D, policy: ProviderExecutionPolicy) -> Self {
        let tool_policy = ToolExecutionPolicy {
            style_pipeline: policy.style_pipeline.clone(),
            plugin_pipeline: policy.plugin_pipeline.clone(),
            user_policy: policy.user_policy.clone(),
            mandatory_policy: policy.mandatory_policy.clone(),
        };
        Self {
            provider: ProviderExecutionLogic::new(data.clone(), policy.clone()),
            artifacts: ArtifactPersistenceLogic::new(data.clone(), policy.clone()),
            policy,
            tools: ToolExecutionLogic::new(data.clone(), tool_policy),
            data,
            session_gates: Arc::new(Mutex::new(BTreeMap::new())),
            child_sessions: None,
            plugins: None,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "the planner adapter keeps graph control adjacent to canonical plan, child, join, and review effects"
    )]
    async fn run_planner_worker_reviewer(
        &self,
        command: RunTurnCommand,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
        scheduled: Option<ScheduledTurnPrelude>,
        session_id: SessionId,
        session_directory: PathBuf,
        persistence: SessionPersistenceLogic<D>,
        preflight: LoadSessionResult,
    ) -> Result<RunTurnResult, RunTurnError> {
        let (user_sequence, mut execution) =
            if let Some(canonical) = preflight.state.style_execution.as_ref() {
                if let Some(reason) = canonical.termination_reason.as_ref() {
                    if reason == "complete_session"
                        && preflight.state.lifecycle == crate::session::SessionLifecycle::Active
                    {
                        let user_sequence = current_run_user_sequence(&preflight.state, &command)?;
                        let (sequence, _) = self.commit_next(
                            &persistence,
                            session_id,
                            &session_directory,
                            preflight.state.last_sequence,
                            preflight.last_event_id,
                            RuntimeCommittedEvent::SessionLifecycleChanged(
                                crate::session::SessionLifecycleChangedEvent {
                                    lifecycle: crate::session::SessionLifecycle::Completed,
                                    reason: Some(String::from("planner-worker-reviewer approved")),
                                },
                            ),
                        )?;
                        return Ok(RunTurnResult {
                            events: Vec::new(),
                            first_committed_sequence: user_sequence,
                            last_committed_sequence: sequence,
                            awaiting_continuation: None,
                        });
                    }
                    return Err(RunTurnError::StyleExecutionTerminalReason(reason.clone()));
                }
                let user_sequence = current_run_user_sequence(&preflight.state, &command)?;
                let execution = Self::resume_active_style_turn(
                    &preflight.state,
                    JournalPosition {
                        sequence: preflight.state.last_sequence,
                        event_id: preflight.last_event_id,
                    },
                )?
                .ok_or(RunTurnError::StyleGraphMismatch)?;
                (user_sequence, execution)
            } else {
                if let Some(scheduled) = scheduled {
                    self.commit_scheduler_fired(
                        &persistence,
                        session_id,
                        &session_directory,
                        scheduled,
                    )?;
                }
                let (state, user_sequence, user_event) =
                    self.commit_user(&persistence, session_id, &session_directory, &command)?;
                let execution = self.begin_style_turn(
                    &persistence,
                    session_id,
                    &session_directory,
                    &state,
                    JournalPosition {
                        sequence: user_sequence,
                        event_id: user_event.metadata.event_id,
                    },
                )?;
                (user_sequence, execution)
            };
        if execution.executor.adapter_kind() != Some(StyleAdapterKind::PlannerWorkerReviewer) {
            return Err(RunTurnError::StyleGraphMismatch);
        }
        validate_planner_child_policy(execution.executor.compiled())?;
        let mut visible_events = Vec::new();

        loop {
            match execution.current.directive {
                StyleNodeDirective::ModelCall if execution.current.id == "plan" => {
                    let state = Self::load_state(&persistence, session_id, &session_directory)?;
                    if state.planner_worker.plan_committed_at.is_none() {
                        let phase_command =
                            planner_phase_command(&command, "plan", execution.loop_iteration);
                        let events = self
                            .execute_planner_model_node(
                                &persistence,
                                session_id,
                                &session_directory,
                                &mut execution,
                                &command,
                                &phase_command,
                                sink,
                            )
                            .await?;
                        let tasks = parse_planner_tasks(
                            &events,
                            execution.executor.compiled().child_agents.max_children,
                        )?;
                        let loaded =
                            Self::load_state(&persistence, session_id, &session_directory)?;
                        let model_response_sequence = loaded
                            .style_execution
                            .as_ref()
                            .and_then(|state| state.latest_model_execution.as_ref())
                            .and_then(|evidence| evidence.completed_at)
                            .ok_or(RunTurnError::PlannerOutputInvalid)?;
                        (execution.position.sequence, execution.position.event_id) = self
                            .commit_next(
                                &persistence,
                                session_id,
                                &session_directory,
                                execution.position.sequence,
                                execution.position.event_id,
                                RuntimeCommittedEvent::TaskPlanCommitted(TaskPlanCommittedEvent {
                                    node_id: execution.current.id.clone(),
                                    attempt: execution.attempt,
                                    loop_iteration: execution.loop_iteration,
                                    step: execution.step,
                                    model_response_sequence,
                                    tasks,
                                }),
                            )?;
                    }
                    self.complete_and_enter_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(String::from("planner:task-plan")),
                    )?;
                }
                StyleNodeDirective::SpawnChildAgent => {
                    self.spawn_planner_children(
                        &persistence,
                        session_id,
                        &session_directory,
                        &command,
                        &mut execution,
                    )
                    .await?;
                    let state = Self::load_state(&persistence, session_id, &session_directory)?;
                    let child_count = state
                        .child_agents
                        .values()
                        .filter(|record| {
                            record.identity.loop_iteration == execution.loop_iteration
                                && matches!(
                                    record.state,
                                    ChildAgentState::Active | ChildAgentState::Completed
                                )
                        })
                        .count();
                    self.complete_and_enter_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!("children:{child_count}")),
                    )?;
                }
                StyleNodeDirective::WaitForAgents => {
                    let child_events = self
                        .wait_for_planner_children(
                            &persistence,
                            session_id,
                            &session_directory,
                            &command,
                            &mut execution,
                        )
                        .await?;
                    visible_events.extend(child_events);
                    let state = Self::load_state(&persistence, session_id, &session_directory)?;
                    let child_count = state
                        .child_agents
                        .values()
                        .filter(|record| {
                            record.identity.loop_iteration == execution.loop_iteration
                                && record.state == ChildAgentState::Completed
                        })
                        .count();
                    self.complete_and_enter_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!("children-completed:{child_count}")),
                    )?;
                }
                StyleNodeDirective::ModelCall if execution.current.id == "integrate" => {
                    self.commit_planner_handoffs(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                    )?;
                    let phase_command =
                        planner_phase_command(&command, "integrate", execution.loop_iteration);
                    let events = self
                        .execute_planner_model_node(
                            &persistence,
                            session_id,
                            &session_directory,
                            &mut execution,
                            &phase_command,
                            &phase_command,
                            sink,
                        )
                        .await?;
                    if !assistant_already_committed(
                        &Self::load_state(&persistence, session_id, &session_directory)?,
                        &phase_command.cancellation_id,
                        &events,
                    ) {
                        execution.position = self.commit_visible_assistant(
                            &persistence,
                            session_id,
                            session_directory.clone(),
                            execution.position.sequence,
                            execution.position.event_id,
                            &phase_command.cancellation_id,
                            &events,
                        )?;
                    }
                    visible_events.extend(events);
                    let loop_iteration = execution.loop_iteration;
                    self.complete_and_enter_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!("integration:iteration:{loop_iteration}")),
                    )?;
                }
                StyleNodeDirective::Review => {
                    let state = Self::load_state(&persistence, session_id, &session_directory)?;
                    let existing = state
                        .planner_worker
                        .reviews
                        .iter()
                        .find(|review| review.loop_iteration == execution.loop_iteration)
                        .cloned();
                    let review = if let Some(review) = existing {
                        (review.approved, review.rejected_task_ids, review.findings)
                    } else {
                        let phase_command =
                            planner_phase_command(&command, "review", execution.loop_iteration);
                        let events = self
                            .execute_planner_model_node(
                                &persistence,
                                session_id,
                                &session_directory,
                                &mut execution,
                                &phase_command,
                                &phase_command,
                                sink,
                            )
                            .await?;
                        let review = parse_reviewer_findings(
                            &events,
                            &Self::load_state(&persistence, session_id, &session_directory)?
                                .planner_worker
                                .tasks,
                        )?;
                        (execution.position.sequence, execution.position.event_id) = self
                            .commit_next(
                                &persistence,
                                session_id,
                                &session_directory,
                                execution.position.sequence,
                                execution.position.event_id,
                                RuntimeCommittedEvent::ReviewerFindingsCommitted(
                                    ReviewerFindingsCommittedEvent {
                                        node_id: execution.current.id.clone(),
                                        attempt: execution.attempt,
                                        loop_iteration: execution.loop_iteration,
                                        step: execution.step,
                                        approved: review.0,
                                        rejected_task_ids: review.1.clone(),
                                        findings: review.2.clone(),
                                    },
                                ),
                            )?;
                        review
                    };
                    self.complete_and_enter_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!("review:approved:{}", review.0)),
                    )?;
                }
                StyleNodeDirective::Loop => {
                    let state = Self::load_state(&persistence, session_id, &session_directory)?;
                    let review = state
                        .planner_worker
                        .reviews
                        .last()
                        .filter(|review| review.loop_iteration == execution.loop_iteration)
                        .ok_or(RunTurnError::PlannerOutputInvalid)?;
                    let approved = review.approved;
                    self.complete_and_enter_next_with(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!("review:approved:{approved}")),
                        None,
                        &json!({"review":{"approved":approved}}),
                        !approved,
                    )?;
                }
                StyleNodeDirective::CompleteSession => {
                    let completed_iteration = execution.loop_iteration;
                    self.complete_terminal_style_node(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!(
                            "planner-worker-reviewer:approved:{completed_iteration}"
                        )),
                    )?;
                    (execution.position.sequence, execution.position.event_id) = self.commit_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution.position.sequence,
                        execution.position.event_id,
                        RuntimeCommittedEvent::SessionLifecycleChanged(
                            crate::session::SessionLifecycleChangedEvent {
                                lifecycle: crate::session::SessionLifecycle::Completed,
                                reason: Some(String::from("planner-worker-reviewer approved")),
                            },
                        ),
                    )?;
                    return Ok(RunTurnResult {
                        events: visible_events,
                        first_committed_sequence: user_sequence,
                        last_committed_sequence: execution.position.sequence,
                        awaiting_continuation: None,
                    });
                }
                _ => {
                    return Err(RunTurnError::UnexpectedStyleNode {
                        expected: "planner-worker-reviewer node",
                        actual: execution.current.id.clone(),
                    });
                }
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "model execution binds the exact graph cursor, context boundary, provider request, and stream sink"
    )]
    async fn execute_planner_model_node(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        execution: &mut ActiveStyleTurn,
        context_command: &RunTurnCommand,
        provider_command: &RunTurnCommand,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
    ) -> Result<Vec<ProviderEvent>, RunTurnError> {
        let state = Self::load_state(persistence, session_id, session_directory)?;
        if let Some(events) = recoverable_research_model_events(&state, provider_command) {
            return Ok(events);
        }
        let context_request_hash = current_context_request_hash(&state, context_command)?;
        let needs_turn_start = state.style_execution.as_ref().is_none_or(|style| {
            style.context_boundaries.iter().rev().all(|boundary| {
                boundary.identity.node_id != execution.current.id
                    || boundary.identity.boundary != "turn_start"
                    || boundary.identity.run_id != context_command.cancellation_id
                    || boundary.identity.request_hash != context_request_hash
                    || boundary.completed_at.is_none()
            })
        });
        if !needs_turn_start && !recoverable_context_retry(&state, context_command) {
            return Err(RunTurnError::StyleRecoveryRequired(
                execution.current.id.clone(),
            ));
        }
        let (state, position) = if needs_turn_start {
            self.compose_style_context(
                persistence,
                session_id,
                session_directory,
                state,
                execution.position,
                context_command,
                ContextCompositionBoundary::TurnStart,
                ContextCompositionOrigin::UserTurn,
            )
            .await?
        } else {
            (state, execution.position)
        };
        let (state, position) = self
            .compose_style_context(
                persistence,
                session_id,
                session_directory,
                state,
                position,
                context_command,
                ContextCompositionBoundary::BeforeModelRequest,
                ContextCompositionOrigin::UserTurn,
            )
            .await?;
        let authorized = self
            .authorize_and_commit(
                persistence,
                session_id,
                session_directory,
                position,
                state,
                provider_command,
            )
            .await?;
        let (events, observed) = self
            .execute_and_commit(
                persistence,
                session_id,
                session_directory,
                authorized,
                &provider_command.cancellation_id,
                sink,
            )
            .await?;
        execution.position = observed;
        if let Some(reason) = provider_node_failure(&events) {
            execution.position = self.fail_style_node_at_head(
                persistence,
                session_id,
                session_directory,
                execution,
                reason,
                Some("planner_model_failed"),
            )?;
            return Err(RunTurnError::PlannerModelFailed);
        }
        if events
            .iter()
            .any(|event| matches!(event, ProviderEvent::ToolProposed { .. }))
        {
            return Err(RunTurnError::PlannerOutputInvalid);
        }
        Ok(events)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "child creation keeps proposal, policy, exact recovery, binding restriction, and atomic receipt adjacent"
    )]
    async fn spawn_planner_children(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        command: &RunTurnCommand,
        execution: &mut ActiveStyleTurn,
    ) -> Result<(), RunTurnError> {
        let policy = execution.executor.compiled().child_agents.clone();
        let child_style = policy
            .child_style
            .clone()
            .ok_or(RunTurnError::InvalidChildPolicy)?;
        let workspace_mode = policy
            .workspace_mode
            .map(child_workspace_mode)
            .ok_or(RunTurnError::InvalidChildPolicy)?;
        let context_budget_tokens = policy
            .context_budget_tokens
            .ok_or(RunTurnError::InvalidChildPolicy)?;
        let memory_access = policy
            .memory_access
            .ok_or(RunTurnError::InvalidChildPolicy)?;
        loop {
            let state = Self::load_state(persistence, session_id, session_directory)?;
            let tasks = planner_tasks_for_iteration(&state, execution.loop_iteration)?;
            let pending = tasks.into_iter().find(|task| {
                !state.child_agents.values().any(|record| {
                    record.identity.loop_iteration == execution.loop_iteration
                        && record.identity.task_id == task.task_id
                        && matches!(
                            record.state,
                            ChildAgentState::Active | ChildAgentState::Completed
                        )
                })
            });
            let Some(task) = pending else {
                return Ok(());
            };
            let identity = ChildAgentExecutionIdentity {
                execution_id: format!(
                    "child:{}:{}:{}:{}",
                    execution.current.id, execution.loop_iteration, task.task_id, execution.step
                ),
                node_id: execution.current.id.clone(),
                attempt: execution.attempt,
                loop_iteration: execution.loop_iteration,
                step: execution.step,
                task_id: task.task_id.clone(),
            };
            let existing = state.child_agents.get(&identity.execution_id).cloned();
            let proposed_at = if let Some(record) = existing.as_ref() {
                record.proposed_at
            } else {
                let next = execution
                    .position
                    .sequence
                    .checked_next()
                    .map_err(|_| RunTurnError::SequenceOverflow)?;
                (execution.position.sequence, execution.position.event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    execution.position.sequence,
                    execution.position.event_id,
                    RuntimeCommittedEvent::ChildAgentCreationProposed(
                        ChildAgentCreationProposedEvent {
                            identity: identity.clone(),
                            task: task.description.clone(),
                            child_style: child_style.clone(),
                            workspace_mode: workspace_mode.clone(),
                            token_budget: policy.per_child_token_budget,
                        },
                    ),
                )?;
                next
            };
            let state = Self::load_state(persistence, session_id, session_directory)?;
            let record = state
                .child_agents
                .get(&identity.execution_id)
                .ok_or(RunTurnError::InvalidChildPolicy)?;
            if record.state == ChildAgentState::Proposed {
                if existing.is_some() {
                    return Err(RunTurnError::StyleControlRecoveryRequired {
                        node: execution.current.id.clone(),
                        phase: "child_creation_policy",
                    });
                }
                let proposal = ActionProposal {
                    id: ProposalId(identity.execution_id.clone()),
                    action: ConsequentialAction::ChildAgentCreation {
                        style: child_style.clone(),
                        workspace_mode: workspace_mode.clone(),
                        token_budget: policy.per_child_token_budget,
                    },
                    style: state.style.clone(),
                    workspace: state.workspace.clone(),
                    origin: String::from("runtime"),
                };
                self.authorize_style_action(proposal.clone(), "child session creation")
                    .await?;
                let action_digest = proposal.digest().map_err(|_| RunTurnError::Event)?;
                (execution.position.sequence, execution.position.event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    execution.position.sequence,
                    execution.position.event_id,
                    RuntimeCommittedEvent::ChildAgentCreationApproved(
                        ChildAgentCreationApprovedEvent {
                            identity: identity.clone(),
                            action_digest,
                        },
                    ),
                )?;
            }
            let state = Self::load_state(persistence, session_id, session_directory)?;
            let record = state
                .child_agents
                .get(&identity.execution_id)
                .ok_or(RunTurnError::InvalidChildPolicy)?;
            if record.state == ChildAgentState::Approved {
                let depth = state
                    .child_origin
                    .as_ref()
                    .map_or(1, |origin| origin.depth.saturating_add(1));
                if depth > u32::from(policy.max_depth) {
                    return Err(RunTurnError::ChildDepthExceeded);
                }
                let child_sessions = self
                    .child_sessions
                    .as_ref()
                    .ok_or(RunTurnError::ChildSessionsUnavailable)?;
                let child = child_sessions
                    .ensure_child_session(EnsureChildSessionCommand {
                        sessions_root: command.sessions_root.clone(),
                        parent_session_id: session_id,
                        parent_action_sequence: proposed_at,
                        parent_graph_node_id: execution.current.id.clone(),
                        workspace: state.workspace.clone(),
                        style_selector: child_style.clone(),
                        task_id: task.task_id.clone(),
                        revision: execution.loop_iteration,
                        depth,
                        task: task.description,
                        token_budget: policy.per_child_token_budget,
                        context_budget_tokens,
                        tool_groups: policy.tool_groups.clone(),
                        memory_access,
                    })
                    .map_err(RunTurnError::ChildSession)?;
                (execution.position.sequence, execution.position.event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    execution.position.sequence,
                    execution.position.event_id,
                    RuntimeCommittedEvent::ChildAgentCreated(ChildAgentCreatedEvent {
                        identity,
                        child_session_id: child.session_id,
                        parent_action_sequence: proposed_at,
                        child_style: child_style.clone(),
                    }),
                )?;
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "child wait binds the parent cursor, each child journal, typed task command, and terminal parent receipt"
    )]
    async fn wait_for_planner_children(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        command: &RunTurnCommand,
        execution: &mut ActiveStyleTurn,
    ) -> Result<Vec<ProviderEvent>, RunTurnError> {
        let mut visible = Vec::new();
        loop {
            let state = Self::load_state(persistence, session_id, session_directory)?;
            let active = state
                .child_agents
                .values()
                .find(|record| {
                    record.identity.loop_iteration == execution.loop_iteration
                        && record.state == ChildAgentState::Active
                })
                .cloned();
            let Some(record) = active else {
                break;
            };
            let child_session_id = record
                .child_session_id
                .ok_or(RunTurnError::InvalidChildPolicy)?;
            let child_directory = command.sessions_root.join(child_session_id.to_string());
            let child_persistence = SessionPersistenceLogic::new(self.data.clone());
            let before = child_persistence
                .load_session(LoadSessionCommand {
                    session_directory: child_directory.clone(),
                    expected_session_id: child_session_id,
                })
                .map_err(RunTurnError::Persistence)?;
            let child_events =
                if before.state.lifecycle == crate::session::SessionLifecycle::Completed {
                    Vec::new()
                } else {
                    Box::pin(self.run_child_task(RunTurnCommand {
                        sessions_root: command.sessions_root.clone(),
                        session_id: child_session_id.to_string(),
                        prompt: record.task.clone(),
                        provider: command.provider.clone(),
                        model: command.model.clone(),
                        options: command.options.clone(),
                        cancellation_id: planner_child_cancellation_id(
                            &command.cancellation_id,
                            &record.identity.task_id,
                            record.identity.loop_iteration,
                        ),
                    }))
                    .await?
                    .events
                };
            visible.extend(child_events.clone());
            let mut loaded = child_persistence
                .load_session(LoadSessionCommand {
                    session_directory: child_directory.clone(),
                    expected_session_id: child_session_id,
                })
                .map_err(RunTurnError::Persistence)?;
            if loaded.state.lifecycle == crate::session::SessionLifecycle::Active {
                self.commit_next(
                    &child_persistence,
                    child_session_id,
                    &child_directory,
                    loaded.state.last_sequence,
                    loaded.last_event_id,
                    RuntimeCommittedEvent::SessionLifecycleChanged(
                        crate::session::SessionLifecycleChangedEvent {
                            lifecycle: crate::session::SessionLifecycle::Completed,
                            reason: Some(String::from("child task completed")),
                        },
                    ),
                )?;
                loaded = child_persistence
                    .load_session(LoadSessionCommand {
                        session_directory: child_directory,
                        expected_session_id: child_session_id,
                    })
                    .map_err(RunTurnError::Persistence)?;
            }
            let summary = latest_assistant_summary(&loaded.state, &child_events);
            (execution.position.sequence, execution.position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                execution.position.sequence,
                execution.position.event_id,
                RuntimeCommittedEvent::ChildAgentCompleted(ChildAgentCompletedEvent {
                    identity: record.identity,
                    child_session_id,
                    child_head_sequence: loaded.state.last_sequence,
                    summary,
                }),
            )?;
        }
        let state = Self::load_state(persistence, session_id, session_directory)?;
        if !state
            .planner_worker
            .joins
            .iter()
            .any(|join| join.loop_iteration == execution.loop_iteration)
        {
            let mut child_execution_ids = state
                .child_agents
                .values()
                .filter(|record| {
                    record.identity.loop_iteration == execution.loop_iteration
                        && record.state == ChildAgentState::Completed
                })
                .map(|record| record.identity.execution_id.clone())
                .collect::<Vec<_>>();
            child_execution_ids.sort();
            (execution.position.sequence, execution.position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                execution.position.sequence,
                execution.position.event_id,
                RuntimeCommittedEvent::ChildJoinCompleted(ChildJoinCompletedEvent {
                    node_id: execution.current.id.clone(),
                    loop_iteration: execution.loop_iteration,
                    child_execution_ids,
                }),
            )?;
        }
        Ok(visible)
    }

    fn commit_planner_handoffs(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        execution: &mut ActiveStyleTurn,
    ) -> Result<(), RunTurnError> {
        let state = Self::load_state(persistence, session_id, session_directory)?;
        let records = state
            .child_agents
            .values()
            .filter(|record| {
                record.identity.loop_iteration == execution.loop_iteration
                    && record.state == ChildAgentState::Completed
            })
            .cloned()
            .collect::<Vec<_>>();
        for record in records {
            let child_session_id = record
                .child_session_id
                .ok_or(RunTurnError::InvalidChildPolicy)?;
            let entry_id = ConversationEntryId(format!(
                "child-handoff:{}:{}",
                record.identity.execution_id, child_session_id
            ));
            let loaded = Self::load_state(persistence, session_id, session_directory)?;
            if loaded
                .conversation
                .history()
                .iter()
                .any(|entry| entry.id() == &entry_id)
            {
                continue;
            }
            let sequence = execution
                .position
                .sequence
                .checked_next()
                .map_err(|_| RunTurnError::SequenceOverflow)?;
            (execution.position.sequence, execution.position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                execution.position.sequence,
                execution.position.event_id,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent {
                        entry: ConversationEntry::ChildAgentHandoff(ChildHandoffEntry {
                            id: entry_id,
                            child_session: child_session_id.to_string(),
                            summary: record.summary.unwrap_or_default(),
                            artifact_id: None,
                            source_sequence: sequence,
                        }),
                    },
                ),
            )?;
        }
        Ok(())
    }

    /// Enables runtime-managed child sessions for styles that declare them.
    #[must_use]
    pub fn with_child_sessions(
        mut self,
        child_sessions: impl ChildSessionLogicPort + 'static,
    ) -> Self {
        self.child_sessions = Some(Arc::new(child_sessions));
        self
    }
}

impl<D> TurnLogic<D> {
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
        + MemoryDataPort
        + ArtifactDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
{
    async fn run_turn(&self, command: RunTurnCommand) -> Result<RunTurnResult, RunTurnError> {
        self.run_turn_internal(command, None, None, TurnInputOrigin::User)
            .await
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
            TurnInputOrigin::User,
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
            match logic
                .run_turn_internal(command, Some(&sender), None, TurnInputOrigin::User)
                .await
            {
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

#[async_trait]
impl<D> CommittedEventObserverLogicPort for TurnLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + MemoryDataPort
        + ArtifactDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
{
    async fn observe_committed_events(
        &self,
        command: ObserveCommittedEventsCommand,
    ) -> Result<PluginObservationSummary, RunTurnError> {
        if command.events.is_empty() {
            return Ok(PluginObservationSummary::default());
        }
        let session_id =
            SessionId::from_str(&command.session_id).map_err(|_| RunTurnError::InvalidSession)?;
        let session_directory = command.sessions_root.join(session_id.to_string());
        let state = Self::load_state(
            &SessionPersistenceLogic::new(self.data.clone()),
            session_id,
            &session_directory,
        )?;
        let binding = state
            .style_binding
            .ok_or(RunTurnError::StyleMigrationRequired)?;
        let compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&binding.compiled_style_json)
                .map_err(|_| RunTurnError::StyleBindingInvalid)?;
        if compiled
            .allowed_plugins
            .iter()
            .all(|plugin| plugin == "runtime" || plugin.starts_with("runtime."))
        {
            return Ok(PluginObservationSummary::default());
        }
        let plugins = self
            .plugins
            .as_ref()
            .ok_or(RunTurnError::PluginCompositionUnavailable)?;
        let last_sequence = command.events.last().map_or(0, |event| event.sequence);
        plugins
            .observe_committed_events(ObserveCommittedPluginEventsCommand {
                session_id: command.session_id,
                cancellation_id: format!("observer-range-{last_sequence}"),
                compiled_style: compiled,
                runtime_api_version: String::from(RUNTIME_PLUGIN_API_VERSION),
                events: command.events,
            })
            .await
            .map_err(RunTurnError::PluginComposition)
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
        + MemoryDataPort
        + ArtifactDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
{
    /// Executes the exact canonical task assigned to a runtime-managed child.
    ///
    /// # Errors
    ///
    /// Returns [`RunTurnError`] when the typed task identity, selected style,
    /// context lifecycle, or an existing effect-safe turn path fails.
    pub async fn run_child_task(
        &self,
        command: RunTurnCommand,
    ) -> Result<RunTurnResult, RunTurnError> {
        self.run_turn_internal(command, None, None, TurnInputOrigin::ChildTask)
            .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the style executor adapter keeps canonical node events adjacent to the existing effect-safe provider and tool phases"
    )]
    async fn run_turn_internal(
        &self,
        command: RunTurnCommand,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
        scheduled: Option<ScheduledTurnPrelude>,
        input_origin: TurnInputOrigin,
    ) -> Result<RunTurnResult, RunTurnError> {
        validate(&command)?;
        let session_id =
            SessionId::from_str(&command.session_id).map_err(|_| RunTurnError::InvalidSession)?;
        let gate = self.session_gate(&command.session_id).await;
        let _session_guard = gate.lock().await;
        let session_directory = command.sessions_root.join(session_id.to_string());
        let persistence = SessionPersistenceLogic::new(self.data.clone());
        let preflight = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.clone(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        let preflight = self.recover_style_control_gaps(
            &persistence,
            session_id,
            &session_directory,
            preflight,
        )?;
        if let Some(visible_text) = recoverable_ephemeral_pre_assistant(&preflight.state, &command)
        {
            let first_committed_sequence = current_run_user_sequence(&preflight.state, &command)?;
            let mut execution = Self::resume_active_style_turn(
                &preflight.state,
                JournalPosition {
                    sequence: preflight.state.last_sequence,
                    event_id: preflight.last_event_id,
                },
            )?
            .ok_or(RunTurnError::StyleGraphMismatch)?;
            let recovered_events = (!visible_text.is_empty())
                .then_some(ProviderEvent::Text(visible_text))
                .into_iter()
                .collect::<Vec<_>>();
            let assistant_position = self.commit_visible_assistant(
                &persistence,
                session_id,
                session_directory.clone(),
                execution.position.sequence,
                execution.position.event_id,
                &command.cancellation_id,
                &recovered_events,
            )?;
            let position = self
                .discard_ephemeral_projection(
                    &persistence,
                    session_id,
                    &session_directory,
                    assistant_position,
                    &command,
                )
                .await?;
            execution.position = position;
            self.complete_terminal_style_node(
                &persistence,
                session_id,
                &session_directory,
                &mut execution,
                Some(format!(
                    "turn-assistant-recovered:{}",
                    command.cancellation_id
                )),
            )?;
            return Ok(RunTurnResult {
                events: Vec::new(),
                first_committed_sequence,
                last_committed_sequence: execution.position.sequence,
                awaiting_continuation: None,
            });
        }
        if recoverable_ephemeral_cleanup_retry(&preflight.state, &command)
            || recoverable_ephemeral_discard(&preflight.state, &command)
            || recoverable_ephemeral_discard_phase(&preflight.state, &command)
        {
            let first_committed_sequence = current_run_user_sequence(&preflight.state, &command)?;
            let mut execution = Self::resume_active_style_turn(
                &preflight.state,
                JournalPosition {
                    sequence: preflight.state.last_sequence,
                    event_id: preflight.last_event_id,
                },
            )?
            .ok_or(RunTurnError::StyleGraphMismatch)?;
            let position = self
                .discard_ephemeral_projection(
                    &persistence,
                    session_id,
                    &session_directory,
                    execution.position,
                    &command,
                )
                .await?;
            execution.position = position;
            self.complete_terminal_style_node(
                &persistence,
                session_id,
                &session_directory,
                &mut execution,
                Some(format!("turn-recovered:{}", command.cancellation_id)),
            )?;
            return Ok(RunTurnResult {
                events: Vec::new(),
                first_committed_sequence,
                last_committed_sequence: execution.position.sequence,
                awaiting_continuation: None,
            });
        }
        if preflight
            .state
            .style_binding
            .as_ref()
            .and_then(|binding| CompiledStyleExecutor::from_binding(binding).ok())
            .and_then(|executor| executor.adapter_kind())
            == Some(StyleAdapterKind::ResearchLoop)
        {
            return self
                .run_research_loop(
                    command,
                    sink,
                    scheduled,
                    session_id,
                    session_directory,
                    persistence,
                    preflight,
                )
                .await;
        }
        if preflight
            .state
            .style_binding
            .as_ref()
            .and_then(|binding| CompiledStyleExecutor::from_binding(binding).ok())
            .and_then(|executor| executor.adapter_kind())
            == Some(StyleAdapterKind::PlannerWorkerReviewer)
        {
            return self
                .run_planner_worker_reviewer(
                    command,
                    sink,
                    scheduled,
                    session_id,
                    session_directory,
                    persistence,
                    preflight,
                )
                .await;
        }
        if preflight
            .state
            .style_binding
            .as_ref()
            .and_then(|binding| CompiledStyleExecutor::from_binding(binding).ok())
            .and_then(|executor| executor.adapter_kind())
            == Some(StyleAdapterKind::DeclarativeGraph)
        {
            return self
                .run_declarative_graph(
                    command,
                    scheduled,
                    session_id,
                    session_directory,
                    persistence,
                    preflight,
                )
                .await;
        }
        let (style_driven, resume_context) = match preflight.state.style_binding.as_ref() {
            Some(binding) => {
                let executor = CompiledStyleExecutor::from_binding(binding)
                    .map_err(RunTurnError::StyleExecutor)?;
                if executor.adapter_kind().is_none() {
                    return Err(RunTurnError::UnsupportedStyleExecution(binding.id.clone()));
                }
                match binding.memory.retrieval_timing.as_str() {
                    "never"
                    | "turnstart"
                    | "turn_start"
                    | "iterationstart"
                    | "iteration_start"
                    | "beforemodelrequest"
                    | "before_model_request" => {}
                    unsupported => {
                        return Err(RunTurnError::UnsupportedMemoryRetrievalTiming(
                            unsupported.to_owned(),
                        ));
                    }
                }
                if let Some(execution) = &preflight.state.style_execution {
                    if let Some(active) = &execution.active_node {
                        if !recoverable_context_retry(&preflight.state, &command) {
                            return Err(RunTurnError::StyleRecoveryRequired(
                                active.node_id.clone(),
                            ));
                        }
                        if execution.termination_reason.is_some() {
                            return Err(RunTurnError::StyleExecutionTerminal);
                        }
                        (true, true)
                    } else {
                        if execution.termination_reason.is_some() {
                            return Err(RunTurnError::StyleExecutionTerminal);
                        }
                        (true, false)
                    }
                } else {
                    (true, false)
                }
            }
            None => (false, false),
        };
        if let Some(scheduled) = scheduled {
            self.commit_scheduler_fired(&persistence, session_id, &session_directory, scheduled)?;
        }
        let (state, user_sequence, mut style_turn, provider_position) = if resume_context {
            let user_sequence = current_run_input_sequence(&preflight.state, &command)?;
            let position = JournalPosition {
                sequence: preflight.state.last_sequence,
                event_id: preflight.last_event_id,
            };
            let style_turn = Self::resume_active_style_turn(&preflight.state, position)?;
            (preflight.state, user_sequence, style_turn, position)
        } else if input_origin == TurnInputOrigin::ChildTask {
            validate_child_task_input(&preflight.state, &command)?;
            let user_sequence = current_run_input_sequence(&preflight.state, &command)?;
            let position = JournalPosition {
                sequence: preflight.state.last_sequence,
                event_id: preflight.last_event_id,
            };
            let style_turn = style_driven
                .then(|| {
                    self.begin_style_turn(
                        &persistence,
                        session_id,
                        &session_directory,
                        &preflight.state,
                        position,
                    )
                })
                .transpose()?;
            let position = style_turn
                .as_ref()
                .map_or(position, |execution| execution.position);
            (preflight.state, user_sequence, style_turn, position)
        } else {
            let (state, user_sequence, user_event) =
                self.commit_user(&persistence, session_id, &session_directory, &command)?;
            // The service boundary rejects legacy unbound sessions before they
            // reach this path. Keeping the internal fallback preserves replay
            // and focused logic fixtures without silently migrating sessions.
            let style_turn = style_driven
                .then(|| {
                    self.begin_style_turn(
                        &persistence,
                        session_id,
                        &session_directory,
                        &state,
                        JournalPosition {
                            sequence: user_sequence,
                            event_id: user_event.metadata.event_id,
                        },
                    )
                })
                .transpose()?;
            let position = style_turn.as_ref().map_or(
                JournalPosition {
                    sequence: user_sequence,
                    event_id: user_event.metadata.event_id,
                },
                |execution| execution.position,
            );
            (state, user_sequence, style_turn, position)
        };
        let state = if style_turn.is_some() {
            Self::load_state(&persistence, session_id, &session_directory)?
        } else {
            state
        };
        let context_origin = if input_origin == TurnInputOrigin::ChildTask {
            ContextCompositionOrigin::ChildTask
        } else {
            ContextCompositionOrigin::UserTurn
        };
        let (state, provider_position) = if style_turn.is_some() {
            let composed = async {
                let execution = style_turn
                    .as_mut()
                    .ok_or(RunTurnError::StyleGraphMismatch)?;
                match execution.executor.adapter_kind().ok_or_else(|| {
                    RunTurnError::UnsupportedStyleExecution(
                        execution.executor.compiled().style_id.clone(),
                    )
                })? {
                    StyleAdapterKind::PersistentTurn => {
                        if resume_context
                            && latest_context_boundary_at_head(&state)
                                .is_some_and(|identity| identity.boundary == "before_model_request")
                        {
                            return self
                                .compose_style_context(
                                    &persistence,
                                    session_id,
                                    &session_directory,
                                    state,
                                    provider_position,
                                    &command,
                                    ContextCompositionBoundary::BeforeModelRequest,
                                    context_origin,
                                )
                                .await;
                        }
                        let (state, position) = self
                            .compose_style_context(
                                &persistence,
                                session_id,
                                &session_directory,
                                state,
                                provider_position,
                                &command,
                                ContextCompositionBoundary::TurnStart,
                                context_origin,
                            )
                            .await?;
                        self.compose_style_context(
                            &persistence,
                            session_id,
                            &session_directory,
                            state,
                            position,
                            &command,
                            ContextCompositionBoundary::BeforeModelRequest,
                            context_origin,
                        )
                        .await
                    }
                    StyleAdapterKind::EphemeralTurn | StyleAdapterKind::ResearchLoop => {
                        let (state, position) = match execution.current.directive {
                            StyleNodeDirective::ContextTransform => {
                                let (_state, position) = self
                                    .compose_style_context(
                                        &persistence,
                                        session_id,
                                        &session_directory,
                                        state,
                                        provider_position,
                                        &command,
                                        ContextCompositionBoundary::TurnStart,
                                        context_origin,
                                    )
                                    .await?;
                                execution.position = position;
                                self.complete_and_enter_next(
                                    &persistence,
                                    session_id,
                                    &session_directory,
                                    execution,
                                    Some(format!("fresh-context:{}", command.cancellation_id)),
                                )?;
                                if execution.current.directive != StyleNodeDirective::ModelCall {
                                    return Err(RunTurnError::UnexpectedStyleNode {
                                        expected: "model_call",
                                        actual: execution.current.id.clone(),
                                    });
                                }
                                (
                                    Self::load_state(&persistence, session_id, &session_directory)?,
                                    execution.position,
                                )
                            }
                            StyleNodeDirective::ModelCall => (state, provider_position),
                            _ => {
                                return Err(RunTurnError::UnexpectedStyleNode {
                                    expected: "context_transform or model_call",
                                    actual: execution.current.id.clone(),
                                });
                            }
                        };
                        self.compose_style_context(
                            &persistence,
                            session_id,
                            &session_directory,
                            state,
                            position,
                            &command,
                            ContextCompositionBoundary::BeforeModelRequest,
                            context_origin,
                        )
                        .await
                    }
                    StyleAdapterKind::PlannerWorkerReviewer
                    | StyleAdapterKind::DeclarativeGraph => Err(RunTurnError::StyleGraphMismatch),
                }
            }
            .await;
            match composed {
                Ok(value) => value,
                Err(error) => {
                    if let Some(execution) = style_turn.as_ref() {
                        self.fail_style_node_at_head(
                            &persistence,
                            session_id,
                            &session_directory,
                            execution,
                            "context_composition_failed",
                            None,
                        )?;
                    }
                    return Err(error);
                }
            }
        } else {
            (state, provider_position)
        };
        let authorized = match self
            .authorize_and_commit(
                &persistence,
                session_id,
                &session_directory,
                provider_position,
                state,
                &command,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                if let Some(execution) = style_turn.as_ref() {
                    self.fail_style_node_at_head(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution,
                        "model_authorization_failed",
                        None,
                    )?;
                }
                return Err(error);
            }
        };
        let (events, observed) = match self
            .execute_and_commit(
                &persistence,
                session_id,
                &session_directory,
                authorized,
                &command.cancellation_id,
                sink,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                if let Some(execution) = style_turn.as_ref() {
                    self.fail_style_node_at_head(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution,
                        "model_execution_failed",
                        None,
                    )?;
                }
                return Err(error);
            }
        };
        if let Some(execution) = style_turn.as_mut() {
            execution.position = observed;
            if let Some(reason) = provider_node_failure(&events) {
                execution.position = self.fail_style_node_at_head(
                    &persistence,
                    session_id,
                    &session_directory,
                    execution,
                    reason,
                    None,
                )?;
                return Ok(RunTurnResult {
                    events,
                    first_committed_sequence: user_sequence,
                    last_committed_sequence: execution.position.sequence,
                    awaiting_continuation: None,
                });
            }
            self.complete_and_enter_next(
                &persistence,
                session_id,
                &session_directory,
                execution,
                Some(format!("model:{}", command.cancellation_id)),
            )?;
            if execution.current.directive != StyleNodeDirective::ToolExecutionGate {
                return Err(RunTurnError::UnexpectedStyleNode {
                    expected: "tool_execution_gate",
                    actual: execution.current.id.clone(),
                });
            }
        }
        let tool_outcome = match self
            .resolve_tool_calls(
                &persistence,
                session_id,
                &session_directory,
                &command,
                events,
                style_turn
                    .as_ref()
                    .map_or(observed, |execution| execution.position),
                sink,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                if let Some(execution) = style_turn.as_ref() {
                    self.fail_style_node_at_head(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution,
                        "tool_gate_failed",
                        None,
                    )?;
                }
                return Err(error);
            }
        };
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
        if let Some(execution) = style_turn.as_mut() {
            execution.position = observed;
            if let Some(reason) = provider_node_failure(&events) {
                execution.position = self.fail_style_node_at_head(
                    &persistence,
                    session_id,
                    &session_directory,
                    execution,
                    reason,
                    None,
                )?;
                return Ok(RunTurnResult {
                    events,
                    first_committed_sequence: user_sequence,
                    last_committed_sequence: execution.position.sequence,
                    awaiting_continuation: None,
                });
            }
            self.complete_and_enter_next(
                &persistence,
                session_id,
                &session_directory,
                execution,
                Some(String::from("tool-gate:complete")),
            )?;
            if execution.current.directive != StyleNodeDirective::CompleteTurn {
                return Err(RunTurnError::UnexpectedStyleNode {
                    expected: "complete_turn",
                    actual: execution.current.id.clone(),
                });
            }
        }
        let assistant_events = Self::assistant_events_for_completion(
            &persistence,
            session_id,
            &session_directory,
            style_turn.as_ref(),
            &command,
            &events,
        )?;
        let assistant_position = self.commit_visible_assistant(
            &persistence,
            session_id,
            session_directory.clone(),
            style_turn
                .as_ref()
                .map_or(observed.sequence, |value| value.position.sequence),
            style_turn
                .as_ref()
                .map_or(observed.event_id, |value| value.position.event_id),
            &command.cancellation_id,
            &assistant_events,
        )?;
        let assistant_position = if style_turn.as_ref().is_some_and(|execution| {
            execution.executor.adapter_kind() == Some(StyleAdapterKind::EphemeralTurn)
        }) {
            self.discard_ephemeral_projection(
                &persistence,
                session_id,
                &session_directory,
                assistant_position,
                &command,
            )
            .await?
        } else {
            assistant_position
        };
        let last_sequence = if let Some(execution) = style_turn.as_mut() {
            execution.position = assistant_position;
            self.complete_terminal_style_node(
                &persistence,
                session_id,
                &command.sessions_root.join(session_id.to_string()),
                execution,
                Some(format!("turn:{}", command.cancellation_id)),
            )?;
            execution.position.sequence
        } else {
            assistant_position.sequence
        };
        Ok(RunTurnResult {
            events,
            first_committed_sequence: user_sequence,
            last_committed_sequence: last_sequence,
            awaiting_continuation: None,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "the research adapter keeps each compiled node adjacent to its existing effect-safe runtime path"
    )]
    async fn run_research_loop(
        &self,
        command: RunTurnCommand,
        sink: Option<&mpsc::Sender<Result<RunTurnStreamItem, RunTurnError>>>,
        scheduled: Option<ScheduledTurnPrelude>,
        session_id: SessionId,
        session_directory: PathBuf,
        persistence: SessionPersistenceLogic<D>,
        preflight: LoadSessionResult,
    ) -> Result<RunTurnResult, RunTurnError> {
        let (user_sequence, mut execution) =
            if let Some(canonical) = preflight.state.style_execution.as_ref() {
                if let Some(reason) = canonical.termination_reason.as_ref() {
                    if reason == "complete_session"
                        && preflight.state.lifecycle == crate::session::SessionLifecycle::Active
                    {
                        let user_sequence = current_run_user_sequence(&preflight.state, &command)?;
                        let (sequence, _) = self.commit_next(
                            &persistence,
                            session_id,
                            &session_directory,
                            preflight.state.last_sequence,
                            preflight.last_event_id,
                            RuntimeCommittedEvent::SessionLifecycleChanged(
                                crate::session::SessionLifecycleChangedEvent {
                                    lifecycle: crate::session::SessionLifecycle::Completed,
                                    reason: Some(String::from("research criteria satisfied")),
                                },
                            ),
                        )?;
                        return Ok(RunTurnResult {
                            events: Vec::new(),
                            first_committed_sequence: user_sequence,
                            last_committed_sequence: sequence,
                            awaiting_continuation: None,
                        });
                    }
                    return Err(RunTurnError::StyleExecutionTerminalReason(reason.clone()));
                }
                validate_research_resume_request(&preflight.state, &command)?;
                let user_sequence = current_run_user_sequence(&preflight.state, &command)?;
                let execution = Self::resume_active_style_turn(
                    &preflight.state,
                    JournalPosition {
                        sequence: preflight.state.last_sequence,
                        event_id: preflight.last_event_id,
                    },
                )?
                .ok_or(RunTurnError::StyleGraphMismatch)?;
                (user_sequence, execution)
            } else {
                if let Some(scheduled) = scheduled {
                    self.commit_scheduler_fired(
                        &persistence,
                        session_id,
                        &session_directory,
                        scheduled,
                    )?;
                }
                let (state, user_sequence, user_event) =
                    self.commit_user(&persistence, session_id, &session_directory, &command)?;
                let execution = self.begin_style_turn(
                    &persistence,
                    session_id,
                    &session_directory,
                    &state,
                    JournalPosition {
                        sequence: user_sequence,
                        event_id: user_event.metadata.event_id,
                    },
                )?;
                (user_sequence, execution)
            };
        if execution.executor.adapter_kind() != Some(StyleAdapterKind::ResearchLoop) {
            return Err(RunTurnError::StyleGraphMismatch);
        }
        let loop_limit = execution
            .executor
            .compiled()
            .graph
            .nodes
            .iter()
            .find(|node| node.kind == agentmod_graph_engine::NodeKind::Loop)
            .and_then(|node| node.max_iterations)
            .ok_or(RunTurnError::StyleGraphMismatch)?;
        let completion_after = research_completion_after(&command.options, loop_limit)?;
        let mut all_events = Vec::new();

        loop {
            let mut iteration_command = command.clone();
            iteration_command.cancellation_id = research_iteration_cancellation_id(
                &command.cancellation_id,
                execution
                    .loop_iteration
                    .checked_add(1)
                    .ok_or(RunTurnError::SequenceOverflow)?,
            );
            if execution.current.directive == StyleNodeDirective::ContextTransform {
                let state = Self::load_state(&persistence, session_id, &session_directory)?;
                let (_state, position) = self
                    .compose_style_context(
                        &persistence,
                        session_id,
                        &session_directory,
                        state,
                        execution.position,
                        &command,
                        ContextCompositionBoundary::TurnStart,
                        ContextCompositionOrigin::UserTurn,
                    )
                    .await?;
                execution.position = position;
                let loop_iteration = execution.loop_iteration;
                self.complete_and_enter_next(
                    &persistence,
                    session_id,
                    &session_directory,
                    &mut execution,
                    Some(format!(
                        "research-context:{}:{loop_iteration}",
                        command.cancellation_id
                    )),
                )?;
            }
            let mut events = Vec::new();
            if execution.current.directive == StyleNodeDirective::ModelCall {
                let state = Self::load_state(&persistence, session_id, &session_directory)?;
                if let Some(recovered) =
                    recoverable_research_model_events(&state, &iteration_command)
                {
                    events = recovered;
                } else {
                    if !recoverable_context_retry(&state, &command) {
                        return Err(RunTurnError::StyleRecoveryRequired(
                            execution.current.id.clone(),
                        ));
                    }
                    let (state, position) = self
                        .compose_style_context(
                            &persistence,
                            session_id,
                            &session_directory,
                            state,
                            execution.position,
                            &command,
                            ContextCompositionBoundary::BeforeModelRequest,
                            ContextCompositionOrigin::UserTurn,
                        )
                        .await?;
                    let authorized = self
                        .authorize_and_commit(
                            &persistence,
                            session_id,
                            &session_directory,
                            position,
                            state,
                            &iteration_command,
                        )
                        .await?;
                    let (executed, observed) = self
                        .execute_and_commit(
                            &persistence,
                            session_id,
                            &session_directory,
                            authorized,
                            &iteration_command.cancellation_id,
                            sink,
                        )
                        .await?;
                    execution.position = observed;
                    events = executed;
                }
                if let Some(reason) = provider_node_failure(&events) {
                    execution.position = self.fail_style_node_at_head(
                        &persistence,
                        session_id,
                        &session_directory,
                        &execution,
                        reason,
                        Some("research_model_failed"),
                    )?;
                    return Ok(RunTurnResult {
                        events,
                        first_committed_sequence: user_sequence,
                        last_committed_sequence: execution.position.sequence,
                        awaiting_continuation: None,
                    });
                }
                self.complete_and_enter_next(
                    &persistence,
                    session_id,
                    &session_directory,
                    &mut execution,
                    Some(format!(
                        "research-model:{}",
                        iteration_command.cancellation_id
                    )),
                )?;
            }
            if events.is_empty()
                && execution.current.directive != StyleNodeDirective::ContextTransform
            {
                events = recoverable_research_model_events(
                    &Self::load_state(&persistence, session_id, &session_directory)?,
                    &iteration_command,
                )
                .ok_or_else(|| RunTurnError::StyleRecoveryRequired(execution.current.id.clone()))?;
            }
            if execution.current.directive == StyleNodeDirective::ToolExecutionGate {
                let tool_outcome = self
                    .resolve_tool_calls(
                        &persistence,
                        session_id,
                        &session_directory,
                        &iteration_command,
                        events,
                        execution.position,
                        sink,
                    )
                    .await?;
                let (resolved_events, observed) = match tool_outcome {
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
                events = resolved_events;
                execution.position = observed;
                if let Some(reason) = provider_node_failure(&events) {
                    execution.position = self.fail_style_node_at_head(
                        &persistence,
                        session_id,
                        &session_directory,
                        &execution,
                        reason,
                        Some("research_tool_loop_failed"),
                    )?;
                    return Ok(RunTurnResult {
                        events,
                        first_committed_sequence: user_sequence,
                        last_committed_sequence: execution.position.sequence,
                        awaiting_continuation: None,
                    });
                }
                self.complete_and_enter_next(
                    &persistence,
                    session_id,
                    &session_directory,
                    &mut execution,
                    Some(String::from("research-tool-gate:complete")),
                )?;
            }
            if execution.current.directive == StyleNodeDirective::PersistArtifact {
                let state = Self::load_state(&persistence, session_id, &session_directory)?;
                let artifact_started = state.artifact_persistences.values().any(|record| {
                    record.identity.node_id == execution.current.id
                        && record.identity.attempt == execution.attempt
                        && record.identity.loop_iteration == execution.loop_iteration
                        && record.identity.step == execution.step
                });
                if !artifact_started
                    && !research_assistant_committed(
                        &state,
                        &iteration_command.cancellation_id,
                        &events,
                    )
                {
                    execution.position = self.commit_visible_assistant(
                        &persistence,
                        session_id,
                        session_directory.clone(),
                        execution.position.sequence,
                        execution.position.event_id,
                        &iteration_command.cancellation_id,
                        &events,
                    )?;
                }
                let finding =
                    research_finding_bytes(&command.prompt, execution.loop_iteration, &events)?;
                let artifact_reference = self
                    .persist_research_finding(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        finding,
                    )
                    .await?;
                let loop_iteration = execution.loop_iteration;
                self.complete_and_enter_next_with(
                    &persistence,
                    session_id,
                    &session_directory,
                    &mut execution,
                    Some(format!("research-finding:{loop_iteration}")),
                    Some(artifact_reference),
                    &json!({}),
                    false,
                )?;
            }
            if execution.current.directive != StyleNodeDirective::Loop
                && execution.current.directive != StyleNodeDirective::CompleteSession
            {
                return Err(RunTurnError::UnexpectedStyleNode {
                    expected: "persist_artifact, loop, or complete_session",
                    actual: execution.current.id.clone(),
                });
            }
            let completed_iterations = execution
                .loop_iteration
                .checked_add(1)
                .ok_or(RunTurnError::SequenceOverflow)?;
            let criteria_met = completed_iterations >= completion_after;
            if execution.current.directive == StyleNodeDirective::Loop {
                self.complete_and_enter_next_with(
                    &persistence,
                    session_id,
                    &session_directory,
                    &mut execution,
                    Some(format!("completion:criteria_met:{criteria_met}")),
                    None,
                    &json!({"completion":{"criteria_met":criteria_met}}),
                    !criteria_met,
                )?;
            }
            all_events.extend(events);
            if execution.current.directive == StyleNodeDirective::CompleteSession {
                if execution.current.directive != StyleNodeDirective::CompleteSession {
                    return Err(RunTurnError::UnexpectedStyleNode {
                        expected: "complete_session",
                        actual: execution.current.id.clone(),
                    });
                }
                self.complete_terminal_style_node(
                    &persistence,
                    session_id,
                    &session_directory,
                    &mut execution,
                    Some(format!("research:completed:{completed_iterations}")),
                )?;
                (execution.position.sequence, execution.position.event_id) = self.commit_next(
                    &persistence,
                    session_id,
                    &session_directory,
                    execution.position.sequence,
                    execution.position.event_id,
                    RuntimeCommittedEvent::SessionLifecycleChanged(
                        crate::session::SessionLifecycleChangedEvent {
                            lifecycle: crate::session::SessionLifecycle::Completed,
                            reason: Some(String::from("research criteria satisfied")),
                        },
                    ),
                )?;
                return Ok(RunTurnResult {
                    events: all_events,
                    first_committed_sequence: user_sequence,
                    last_committed_sequence: execution.position.sequence,
                    awaiting_continuation: None,
                });
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "the declarative adapter keeps branch, approval, exact tool recovery, bounded loop, and terminal lifecycle adjacent"
    )]
    async fn run_declarative_graph(
        &self,
        command: RunTurnCommand,
        scheduled: Option<ScheduledTurnPrelude>,
        session_id: SessionId,
        session_directory: PathBuf,
        persistence: SessionPersistenceLogic<D>,
        preflight: LoadSessionResult,
    ) -> Result<RunTurnResult, RunTurnError> {
        let (requires_approval, iteration_limit, tool_arguments, request_reference) =
            declarative_inputs(&command.options)?;
        let (user_sequence, mut execution) =
            if let Some(canonical) = preflight.state.style_execution.as_ref() {
                if let Some(reason) = canonical.termination_reason.as_ref() {
                    if reason == "complete_session"
                        && preflight.state.lifecycle == crate::session::SessionLifecycle::Active
                    {
                        let user_sequence = current_run_user_sequence(&preflight.state, &command)?;
                        let (sequence, _) = self.commit_next(
                            &persistence,
                            session_id,
                            &session_directory,
                            preflight.state.last_sequence,
                            preflight.last_event_id,
                            RuntimeCommittedEvent::SessionLifecycleChanged(
                                crate::session::SessionLifecycleChangedEvent {
                                    lifecycle: crate::session::SessionLifecycle::Completed,
                                    reason: Some(String::from("declarative graph completed")),
                                },
                            ),
                        )?;
                        return Ok(RunTurnResult {
                            events: Vec::new(),
                            first_committed_sequence: user_sequence,
                            last_committed_sequence: sequence,
                            awaiting_continuation: None,
                        });
                    }
                    return Err(RunTurnError::StyleExecutionTerminalReason(reason.clone()));
                }
                validate_declarative_resume_request(&preflight.state, &request_reference)?;
                let user_sequence = current_run_user_sequence(&preflight.state, &command)?;
                let execution = Self::resume_active_style_turn(
                    &preflight.state,
                    JournalPosition {
                        sequence: preflight.state.last_sequence,
                        event_id: preflight.last_event_id,
                    },
                )?
                .ok_or(RunTurnError::StyleGraphMismatch)?;
                (user_sequence, execution)
            } else {
                if let Some(scheduled) = scheduled {
                    self.commit_scheduler_fired(
                        &persistence,
                        session_id,
                        &session_directory,
                        scheduled,
                    )?;
                }
                let (state, user_sequence, user_event) =
                    self.commit_user(&persistence, session_id, &session_directory, &command)?;
                let execution = self.begin_style_turn_with_input(
                    &persistence,
                    session_id,
                    &session_directory,
                    &state,
                    JournalPosition {
                        sequence: user_sequence,
                        event_id: user_event.metadata.event_id,
                    },
                    Some(request_reference.clone()),
                )?;
                (user_sequence, execution)
            };
        if execution.executor.adapter_kind() != Some(StyleAdapterKind::DeclarativeGraph) {
            return Err(RunTurnError::StyleGraphMismatch);
        }
        let compiled_loop_limit = execution
            .executor
            .node("repeat")
            .map_err(RunTurnError::StyleExecutor)?
            .max_iterations
            .ok_or(RunTurnError::StyleGraphMismatch)?;
        if iteration_limit > compiled_loop_limit {
            return Err(RunTurnError::InvalidDeclarativeInputs);
        }

        loop {
            match execution.current.directive {
                StyleNodeDirective::ConditionalBranch => {
                    self.complete_and_enter_next_with(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(request_reference.clone()),
                        None,
                        &json!({"request":{"requires_approval":requires_approval}}),
                        false,
                    )?;
                }
                StyleNodeDirective::UserApproval => {
                    let state = Self::load_state(&persistence, session_id, &session_directory)?;
                    let continuation_id =
                        ContinuationId::from_uuid(execution.position.event_id.into_uuid());
                    if state
                        .approvals
                        .get(&continuation_id)
                        .is_some_and(|approval| approval.state == ApprovalState::Pending)
                    {
                        return Ok(RunTurnResult {
                            events: Vec::new(),
                            first_committed_sequence: user_sequence,
                            last_committed_sequence: state.last_sequence,
                            awaiting_continuation: Some(continuation_id.to_string()),
                        });
                    }
                    let binding = state
                        .style_binding
                        .as_ref()
                        .ok_or(RunTurnError::StyleMigrationRequired)?;
                    ContinuationLogic::new(self.data.clone())
                        .create_continuation(CreateContinuationCommand {
                            session_id: command.session_id.clone(),
                            id: continuation_id,
                            wake_condition: ContinuationWakeCondition::Manual,
                            payload: ContinuationPayload::StyleApproval(Box::new(
                                StyleApprovalContinuation {
                                    session_id: command.session_id.clone(),
                                    workspace: state.workspace,
                                    prompt: command.prompt.clone(),
                                    provider: command.provider.clone(),
                                    model: command.model.clone(),
                                    options: command.options.clone(),
                                    style: state.style,
                                    cancellation_id: command.cancellation_id.clone(),
                                    compiled_style_cache_key: binding
                                        .compiled_cache_key
                                        .to_string(),
                                    node_id: execution.current.id.clone(),
                                    attempt: execution.attempt,
                                    loop_iteration: execution.loop_iteration,
                                    step: execution.step,
                                    request_reference: request_reference.clone(),
                                },
                            )),
                            expires_at: None,
                        })
                        .map_err(RunTurnError::Continuation)?;
                    (execution.position.sequence, execution.position.event_id) = self.commit_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution.position.sequence,
                        execution.position.event_id,
                        RuntimeCommittedEvent::ApprovalRequested(ApprovalRequestedEvent {
                            continuation_id,
                            action_summary: String::from(
                                "declarative graph requested user approval",
                            ),
                        }),
                    )?;
                    return Ok(RunTurnResult {
                        events: Vec::new(),
                        first_committed_sequence: user_sequence,
                        last_committed_sequence: execution.position.sequence,
                        awaiting_continuation: Some(continuation_id.to_string()),
                    });
                }
                StyleNodeDirective::ToolExecutionGate => {
                    let tool = execution
                        .current
                        .tool
                        .clone()
                        .ok_or(RunTurnError::StyleGraphMismatch)?;
                    let call_id = format!(
                        "style:{}:{}:{}:{}",
                        execution.current.id,
                        execution.attempt,
                        execution.loop_iteration,
                        execution.step
                    );
                    let outcome = match self
                        .execute_style_owned_tool(
                            &persistence,
                            session_id,
                            &session_directory,
                            &command,
                            execution.position,
                            &call_id,
                            &tool,
                            tool_arguments.clone(),
                        )
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(
                            error @ (RunTurnError::StyleOwnedToolApprovalUnsupported
                            | RunTurnError::StyleOwnedToolReplacementUnsupported),
                        ) => {
                            let loaded = persistence
                                .load_session(LoadSessionCommand {
                                    session_directory: session_directory.clone(),
                                    expected_session_id: session_id,
                                })
                                .map_err(RunTurnError::Persistence)?;
                            execution.position = JournalPosition {
                                sequence: loaded.state.last_sequence,
                                event_id: loaded.last_event_id,
                            };
                            self.fail_style_node_at_head(
                                &persistence,
                                session_id,
                                &session_directory,
                                &execution,
                                "style_tool_policy_unsupported",
                                Some("declarative_tool_policy_unsupported"),
                            )?;
                            return Err(error);
                        }
                        Err(error) => return Err(error),
                    };
                    execution.position = match outcome {
                        ToolCallOutcome::Complete(position) => position,
                        ToolCallOutcome::Cancelled(position) => {
                            execution.position = position;
                            let position = self.fail_style_node_at_head(
                                &persistence,
                                session_id,
                                &session_directory,
                                &execution,
                                "style_tool_cancelled",
                                Some("declarative_tool_cancelled"),
                            )?;
                            return Ok(RunTurnResult {
                                events: Vec::new(),
                                first_committed_sequence: user_sequence,
                                last_committed_sequence: position.sequence,
                                awaiting_continuation: None,
                            });
                        }
                        ToolCallOutcome::Awaiting {
                            position,
                            continuation_id,
                        } => {
                            return Ok(RunTurnResult {
                                events: Vec::new(),
                                first_committed_sequence: user_sequence,
                                last_committed_sequence: position.sequence,
                                awaiting_continuation: Some(continuation_id.to_string()),
                            });
                        }
                    };
                    if execution.current.directive == StyleNodeDirective::ToolExecutionGate {
                        let loop_iteration = execution.loop_iteration;
                        self.complete_and_enter_next(
                            &persistence,
                            session_id,
                            &session_directory,
                            &mut execution,
                            Some(format!(
                                "declarative-tool:{call_id}:iteration:{loop_iteration}"
                            )),
                        )?;
                    }
                }
                StyleNodeDirective::Loop => {
                    let completed_iterations = execution
                        .loop_iteration
                        .checked_add(1)
                        .ok_or(RunTurnError::SequenceOverflow)?;
                    let remaining = completed_iterations < iteration_limit;
                    self.complete_and_enter_next_with(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!("iteration:remaining:{remaining}")),
                        None,
                        &json!({"iteration":{"remaining":remaining}}),
                        remaining,
                    )?;
                }
                StyleNodeDirective::CompleteSession => {
                    let completed_iterations = execution.loop_iteration.saturating_add(1);
                    self.complete_terminal_style_node(
                        &persistence,
                        session_id,
                        &session_directory,
                        &mut execution,
                        Some(format!("declarative:completed:{completed_iterations}")),
                    )?;
                    (execution.position.sequence, execution.position.event_id) = self.commit_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution.position.sequence,
                        execution.position.event_id,
                        RuntimeCommittedEvent::SessionLifecycleChanged(
                            crate::session::SessionLifecycleChangedEvent {
                                lifecycle: crate::session::SessionLifecycle::Completed,
                                reason: Some(String::from("declarative graph completed")),
                            },
                        ),
                    )?;
                    return Ok(RunTurnResult {
                        events: Vec::new(),
                        first_committed_sequence: user_sequence,
                        last_committed_sequence: execution.position.sequence,
                        awaiting_continuation: None,
                    });
                }
                _ => {
                    return Err(RunTurnError::UnexpectedStyleNode {
                        expected: "conditional_branch, user_approval, tool_execution_gate, loop, or complete_session",
                        actual: execution.current.id,
                    });
                }
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "style-owned tool recovery binds the graph cursor, command, and canonical persistence head explicitly"
    )]
    async fn execute_style_owned_tool(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        command: &RunTurnCommand,
        position: JournalPosition,
        call_id: &str,
        tool: &str,
        arguments: Value,
    ) -> Result<ToolCallOutcome, RunTurnError> {
        let state = Self::load_state(persistence, session_id, session_directory)?;
        let Some(record) = state.tool_executions.get(call_id) else {
            return self
                .execute_tool_call(
                    persistence,
                    session_id,
                    session_directory,
                    command,
                    position,
                    call_id,
                    tool,
                    arguments,
                    "style-owned",
                    Vec::new(),
                )
                .await;
        };
        let prepared = self
            .tools
            .prepare(PrepareToolCommand {
                session_id: command.session_id.clone(),
                workspace: PathBuf::from(&state.workspace),
                call_id: call_id.to_owned(),
                tool: tool.to_owned(),
                arguments,
                cancellation_id: command.cancellation_id.clone(),
                style: state.style.clone(),
            })
            .map_err(RunTurnError::Tool)?;
        let action_digest = prepared
            .original
            .digest()
            .map_err(|_| RunTurnError::Event)?;
        if record.action_digest != Some(action_digest) {
            return Err(RunTurnError::InvalidRecoveryReceipt(call_id.to_owned()));
        }
        let ConsequentialAction::ToolCall(action) = &prepared.original.action else {
            return Err(RunTurnError::InvalidContinuationPayload);
        };
        if record.state == ToolExecutionState::Terminal {
            let repaired = self.repair_terminal_tool_conversation(
                persistence,
                session_id,
                session_directory,
                &state,
                position,
                call_id,
                action,
                action_digest,
                record,
            )?;
            return Ok(ToolCallOutcome::Complete(repaired));
        }
        let authorized = self
            .tools
            .approve_pending(PrepareToolCommand {
                session_id: command.session_id.clone(),
                workspace: PathBuf::from(&state.workspace),
                call_id: call_id.to_owned(),
                tool: tool.to_owned(),
                arguments: action.arguments.clone(),
                cancellation_id: command.cancellation_id.clone(),
                style: state.style,
            })
            .map_err(RunTurnError::Tool)?;
        let result = self
            .execute_authorized_tool(
                persistence,
                session_id,
                session_directory,
                position,
                call_id,
                authorized,
                ToolDispatchMode::Reconcile {
                    observed_event_count: record.observed_event_count,
                },
            )
            .await?;
        Ok(if result.cancelled {
            ToolCallOutcome::Cancelled(result.position)
        } else {
            ToolCallOutcome::Complete(result.position)
        })
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "artifact outbox recovery keeps proposal, approval, dispatch, exact-store reconciliation, and terminal evidence adjacent"
    )]
    async fn persist_research_finding(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        execution: &mut ActiveStyleTurn,
        bytes: Vec<u8>,
    ) -> Result<String, RunTurnError> {
        let content_hash = ContentHash::digest(&bytes);
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        if loaded.state.last_sequence != execution.position.sequence
            || loaded.last_event_id != execution.position.event_id
        {
            return Err(RunTurnError::StyleRecoveryRequired(
                execution.current.id.clone(),
            ));
        }
        let existing = loaded
            .state
            .artifact_persistences
            .values()
            .find(|record| {
                record.identity.node_id == execution.current.id
                    && record.identity.attempt == execution.attempt
                    && record.identity.loop_iteration == execution.loop_iteration
                    && record.identity.step == execution.step
            })
            .cloned();
        let (identity, command, mut approved_digest, mut resume_action) =
            if let Some(record) = existing {
                if record.identity.content_hash != content_hash
                    || record.mime_type != "application/json"
                    || record.byte_size
                        != u64::try_from(bytes.len())
                            .map_err(|_| RunTurnError::ResearchArtifactEncoding)?
                {
                    return Err(RunTurnError::StyleRecoveryRequired(
                        execution.current.id.clone(),
                    ));
                }
                let command = PersistArtifactCommand {
                    proposal_id: record.identity.proposal_id.clone(),
                    style: loaded.state.style.clone(),
                    workspace: loaded.state.workspace.clone(),
                    store_root: session_directory.join("artifacts").join("style"),
                    creation_event: record.proposed_event.to_string(),
                    producer: String::from("runtime.style"),
                    mime_type: record.mime_type.clone(),
                    bytes,
                    security: ArtifactSecurity::Private,
                    retention: ArtifactRetention::Session,
                };
                let resume_action = record.resume_action();
                (
                    record.identity,
                    command,
                    record.action_digest,
                    Some(resume_action),
                )
            } else {
                let reserved = self
                    .data
                    .allocate_event_identity(AllocateEventIdentityDataRequest)
                    .map_err(RunTurnError::Identity)?;
                let proposal_id = reserved.event_id.to_string();
                let identity = ArtifactPersistenceIdentity {
                    execution_id: format!(
                        "research:{}:{}:{}:{}",
                        execution.current.id,
                        execution.attempt,
                        execution.loop_iteration,
                        execution.step
                    ),
                    proposal_id: proposal_id.clone(),
                    node_id: execution.current.id.clone(),
                    attempt: execution.attempt,
                    loop_iteration: execution.loop_iteration,
                    step: execution.step,
                    content_hash,
                };
                let command = PersistArtifactCommand {
                    proposal_id,
                    style: loaded.state.style.clone(),
                    workspace: loaded.state.workspace.clone(),
                    store_root: session_directory.join("artifacts").join("style"),
                    creation_event: reserved.event_id.to_string(),
                    producer: String::from("runtime.style"),
                    mime_type: String::from("application/json"),
                    bytes,
                    security: ArtifactSecurity::Private,
                    retention: ArtifactRetention::Session,
                };
                let prepared = self
                    .artifacts
                    .prepare(command.clone())
                    .map_err(RunTurnError::ResearchArtifact)?;
                let sequence = execution
                    .position
                    .sequence
                    .checked_next()
                    .map_err(|_| RunTurnError::SequenceOverflow)?;
                let event = Self::seal_event_with_identity(
                    session_id,
                    sequence,
                    Some(CausationId::from_uuid(
                        execution.position.event_id.into_uuid(),
                    )),
                    reserved,
                    RuntimeCommittedEvent::ArtifactPersistenceProposed(
                        ArtifactPersistenceProposedEvent {
                            identity: identity.clone(),
                            mime_type: command.mime_type.clone(),
                            byte_size: u64::try_from(command.bytes.len())
                                .map_err(|_| RunTurnError::ResearchArtifactEncoding)?,
                        },
                    ),
                )?;
                execution.position = JournalPosition {
                    sequence,
                    event_id: event.metadata.event_id,
                };
                persistence
                    .commit_event(CommitSessionEventCommand {
                        session_directory: session_directory.to_owned(),
                        event,
                        durability: CommitDurability::Data,
                    })
                    .map_err(RunTurnError::Persistence)?;
                let authorized = self
                    .artifacts
                    .authorize_prepared(prepared)
                    .await
                    .map_err(RunTurnError::ResearchArtifact)?;
                let action_digest = authorized
                    .executable
                    .digest()
                    .map_err(|_| RunTurnError::Event)?;
                (execution.position.sequence, execution.position.event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    execution.position.sequence,
                    execution.position.event_id,
                    RuntimeCommittedEvent::ArtifactPersistenceApproved(
                        ArtifactPersistenceApprovedEvent {
                            identity: identity.clone(),
                            action_digest,
                        },
                    ),
                )?;
                let command = command.clone();
                (
                    identity,
                    command,
                    Some(action_digest),
                    Some(ArtifactPersistenceResumeAction::DispatchApproved),
                )
            };

        if resume_action == Some(ArtifactPersistenceResumeAction::AwaitPolicyRecovery) {
            let authorized = self
                .artifacts
                .authorize_prepared(
                    self.artifacts
                        .prepare(command.clone())
                        .map_err(RunTurnError::ResearchArtifact)?,
                )
                .await
                .map_err(RunTurnError::ResearchArtifact)?;
            let action_digest = authorized
                .executable
                .digest()
                .map_err(|_| RunTurnError::Event)?;
            (execution.position.sequence, execution.position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                execution.position.sequence,
                execution.position.event_id,
                RuntimeCommittedEvent::ArtifactPersistenceApproved(
                    ArtifactPersistenceApprovedEvent {
                        identity: identity.clone(),
                        action_digest,
                    },
                ),
            )?;
            approved_digest = Some(action_digest);
            resume_action = Some(ArtifactPersistenceResumeAction::DispatchApproved);
        }
        if resume_action == Some(ArtifactPersistenceResumeAction::CompleteNode) {
            return loaded
                .state
                .artifact_persistences
                .get(&identity.execution_id)
                .and_then(|record| record.artifact_reference.clone())
                .ok_or_else(|| RunTurnError::StyleRecoveryRequired(execution.current.id.clone()));
        }
        let action_digest =
            approved_digest.ok_or_else(|| RunTurnError::StyleControlRecoveryRequired {
                node: execution.current.id.clone(),
                phase: "artifact_policy",
            })?;

        let authorized = self
            .artifacts
            .restore_authorized(command.clone(), action_digest)
            .map_err(RunTurnError::ResearchArtifact)?;
        if resume_action == Some(ArtifactPersistenceResumeAction::DispatchApproved) {
            (execution.position.sequence, execution.position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                execution.position.sequence,
                execution.position.event_id,
                RuntimeCommittedEvent::ArtifactPersistenceDispatched(
                    ArtifactPersistenceDispatchedEvent {
                        identity: identity.clone(),
                        action_digest,
                    },
                ),
            )?;
        }
        let persisted = if resume_action == Some(ArtifactPersistenceResumeAction::ReconcileReceipt)
        {
            self.artifacts
                .reconcile(&command)
                .map_err(RunTurnError::ResearchArtifact)?
                .map_or_else(|| self.artifacts.persist_authorized(authorized), Ok)
                .map_err(RunTurnError::ResearchArtifact)?
        } else {
            self.artifacts
                .persist_authorized(authorized)
                .map_err(RunTurnError::ResearchArtifact)?
        };
        (execution.position.sequence, execution.position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            execution.position.sequence,
            execution.position.event_id,
            RuntimeCommittedEvent::ArtifactPersistenceCompleted(
                ArtifactPersistenceCompletedEvent {
                    identity,
                    action_digest,
                    artifact_id: persisted.artifact_id,
                    artifact_reference: persisted.artifact_reference.clone(),
                    mime_type: persisted.mime_type,
                    byte_size: persisted.byte_size,
                },
            ),
        )?;
        Ok(persisted.artifact_reference)
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
            .cancel(String::new(), command.cancellation_id)
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
        + MemoryDataPort
        + ArtifactDataPort
        + agentmod_runtime_data::tool::ToolDataPort
        + 'static,
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
            let (workspace, style) = match &loaded_continuation.payload {
                ContinuationPayload::ToolApproval(payload) => (&payload.workspace, &payload.style),
                ContinuationPayload::StyleApproval(payload) => (&payload.workspace, &payload.style),
                ContinuationPayload::DeferredTurn(_) | ContinuationPayload::Opaque(_) => {
                    return Err(RunTurnError::InvalidContinuationPayload);
                }
            };
            self.tools
                .authorize_continuation_resume(
                    &command.session_id,
                    workspace,
                    style,
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
        if matches!(&resolved.payload, ContinuationPayload::StyleApproval(_)) {
            let ContinuationPayload::StyleApproval(payload) = resolved.payload else {
                unreachable!("style approval variant was checked")
            };
            if payload.session_id != command.session_id
                || loaded.state.workspace != payload.workspace
                || loaded.state.style != payload.style
                || loaded.state.style_binding.as_ref().is_none_or(|binding| {
                    binding.compiled_cache_key.to_string() != payload.compiled_style_cache_key
                })
                || loaded
                    .state
                    .style_execution
                    .as_ref()
                    .is_none_or(|execution| {
                        execution.input_reference.as_deref()
                            != Some(payload.request_reference.as_str())
                    })
            {
                return Err(RunTurnError::InvalidContinuationPayload);
            }
            let approval = loaded
                .state
                .approvals
                .get(&continuation_id)
                .ok_or(RunTurnError::InvalidContinuationPayload)?;
            let expected_state = if command.approved {
                ApprovalState::Approved
            } else {
                ApprovalState::Denied
            };
            let mut position = JournalPosition {
                sequence: loaded.state.last_sequence,
                event_id: loaded.last_event_id,
            };
            if approval.state == ApprovalState::Pending {
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
            } else if approval.state != expected_state {
                return Err(RunTurnError::InvalidContinuationPayload);
            }
            let loaded = persistence
                .load_session(LoadSessionCommand {
                    session_directory: session_directory.clone(),
                    expected_session_id: session_id,
                })
                .map_err(RunTurnError::Persistence)?;
            if loaded
                .state
                .style_execution
                .as_ref()
                .is_some_and(|execution| {
                    execution.termination_reason.as_deref() == Some("complete_session")
                })
                && loaded.state.lifecycle == crate::session::SessionLifecycle::Completed
                && command.approved
            {
                return Ok(ResolveTurnApprovalResult {
                    transitioned: resolved.transitioned,
                    events: Vec::new(),
                    last_committed_sequence: loaded.state.last_sequence,
                    awaiting_continuation: None,
                });
            }
            if loaded
                .state
                .style_execution
                .as_ref()
                .is_some_and(|execution| {
                    execution.termination_reason.as_deref() == Some("declarative_approval_denied")
                })
                && loaded.state.lifecycle == crate::session::SessionLifecycle::Failed
                && !command.approved
            {
                return Ok(ResolveTurnApprovalResult {
                    transitioned: resolved.transitioned,
                    events: Vec::new(),
                    last_committed_sequence: loaded.state.last_sequence,
                    awaiting_continuation: None,
                });
            }
            let loaded = self.recover_style_control_gaps(
                &persistence,
                session_id,
                &session_directory,
                loaded,
            )?;
            let mut execution = Self::resume_active_style_turn(
                &loaded.state,
                JournalPosition {
                    sequence: loaded.state.last_sequence,
                    event_id: loaded.last_event_id,
                },
            )?
            .ok_or(RunTurnError::StyleGraphMismatch)?;
            if execution.executor.adapter_kind() != Some(StyleAdapterKind::DeclarativeGraph) {
                return Err(RunTurnError::InvalidContinuationPayload);
            }
            if !command.approved {
                validate_style_approval_cursor(&execution, &payload, &continuation_id)?;
                let position = self.fail_style_node_at_head(
                    &persistence,
                    session_id,
                    &session_directory,
                    &execution,
                    "user_denied",
                    Some("declarative_approval_denied"),
                )?;
                let (sequence, _) = self.commit_next(
                    &persistence,
                    session_id,
                    &session_directory,
                    position.sequence,
                    position.event_id,
                    RuntimeCommittedEvent::SessionLifecycleChanged(
                        crate::session::SessionLifecycleChangedEvent {
                            lifecycle: crate::session::SessionLifecycle::Failed,
                            reason: Some(String::from("declarative graph approval denied")),
                        },
                    ),
                )?;
                return Ok(ResolveTurnApprovalResult {
                    transitioned: resolved.transitioned,
                    events: Vec::new(),
                    last_committed_sequence: sequence,
                    awaiting_continuation: None,
                });
            }
            if execution.current.directive == StyleNodeDirective::UserApproval {
                validate_style_approval_cursor(&execution, &payload, &continuation_id)?;
                execution.position = JournalPosition {
                    sequence: loaded.state.last_sequence,
                    event_id: loaded.last_event_id,
                };
                self.complete_and_enter_next(
                    &persistence,
                    session_id,
                    &session_directory,
                    &mut execution,
                    Some(format!("declarative-approval:{continuation_id}")),
                )?;
            }
            let preflight = persistence
                .load_session(LoadSessionCommand {
                    session_directory: session_directory.clone(),
                    expected_session_id: session_id,
                })
                .map_err(RunTurnError::Persistence)?;
            let resumed = RunTurnCommand {
                sessions_root: command.sessions_root,
                session_id: command.session_id,
                prompt: payload.prompt,
                provider: payload.provider,
                model: payload.model,
                options: payload.options,
                cancellation_id: payload.cancellation_id,
            };
            let result = self
                .run_declarative_graph(
                    resumed,
                    None,
                    session_id,
                    session_directory,
                    persistence,
                    preflight,
                )
                .await?;
            return Ok(ResolveTurnApprovalResult {
                transitioned: resolved.transitioned,
                events: result.events,
                last_committed_sequence: result.last_committed_sequence,
                awaiting_continuation: result.awaiting_continuation,
            });
        }
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
        let mut recovery = approval_recovery_action(
            resolved.transitioned,
            resolved.disposition,
            approval_state,
            execution_record.as_ref().map(|execution| execution.state),
        );
        let tool_already_terminal = execution_record
            .as_ref()
            .is_some_and(|execution| execution.state == ToolExecutionState::Terminal);
        if recovery == ApprovalRecoveryAction::Idempotent
            && command.resume_after_resolution
            && tool_already_terminal
            && pending_model_resume_after_terminal_tool(&loaded.state, execution_record.as_ref())?
        {
            recovery = ApprovalRecoveryAction::Resume;
        }
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
        let recovery_action_digest = prepared
            .original
            .digest()
            .map_err(|_| RunTurnError::Event)?;
        let ConsequentialAction::ToolCall(action) = &prepared.original.action else {
            return Err(RunTurnError::InvalidContinuationPayload);
        };
        if resolved.disposition == ApprovalDisposition::Approved {
            if !tool_already_terminal {
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
                    let last_committed_sequence = self
                        .fail_active_bound_style_at_head(
                            &persistence,
                            session_id,
                            &session_directory,
                            "model_request_cancelled",
                        )?
                        .map_or(last_committed_sequence, |position| position.sequence);
                    return Ok(ResolveTurnApprovalResult {
                        transitioned: true,
                        events,
                        last_committed_sequence,
                        awaiting_continuation: None,
                    });
                }
            }
        } else if !tool_already_terminal {
            (position.sequence, position.event_id) = self.commit_tool_failure(
                &persistence,
                session_id,
                &session_directory,
                position,
                &payload.call_id,
                Some(recovery_action_digest),
                "permission_denied",
                "user denied the requested action",
                false,
            )?;
        }
        let state_after_terminal = Self::load_state(&persistence, session_id, &session_directory)?;
        let terminal = state_after_terminal
            .tool_executions
            .get(&payload.call_id)
            .ok_or_else(|| {
                RunTurnError::ToolConversationRecoveryConflict(payload.call_id.clone())
            })?;
        position = self.repair_terminal_tool_conversation(
            &persistence,
            session_id,
            &session_directory,
            &state_after_terminal,
            position,
            &payload.call_id,
            action,
            recovery_action_digest,
            terminal,
        )?;
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
            let last_committed_sequence = self
                .fail_active_bound_style_at_head(
                    &persistence,
                    session_id,
                    &session_directory,
                    "model_request_cancelled",
                )?
                .map_or(last_committed_sequence, |position| position.sequence);
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
                    let last_committed_sequence = self
                        .fail_active_bound_style_at_head(
                            &persistence,
                            session_id,
                            &session_directory,
                            "model_request_cancelled",
                        )?
                        .map_or(last_committed_sequence, |position| position.sequence);
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
        let mut state = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.clone(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?
            .state;
        if state.style_binding.is_some() {
            let composed = self
                .compose_style_context(
                    &persistence,
                    session_id,
                    &session_directory,
                    state,
                    position,
                    &resumed_turn,
                    ContextCompositionBoundary::BeforeModelRequest,
                    ContextCompositionOrigin::ApprovalContinuation,
                )
                .await;
            match composed {
                Ok(value) => (state, position) = value,
                Err(error) => {
                    self.fail_active_bound_style_at_head(
                        &persistence,
                        session_id,
                        &session_directory,
                        "context_composition_failed",
                    )?;
                    return Err(error);
                }
            }
        }
        let mut style_turn = Self::resume_active_style_turn(&state, position)?;
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
                if let Some(execution) = style_turn.as_mut() {
                    if execution.current.directive != StyleNodeDirective::ToolExecutionGate {
                        return Err(RunTurnError::UnexpectedStyleNode {
                            expected: "tool_execution_gate",
                            actual: execution.current.id.clone(),
                        });
                    }
                    execution.position = position;
                    if let Some(reason) = provider_node_failure(&events) {
                        execution.position = self.fail_style_node_at_head(
                            &persistence,
                            session_id,
                            &session_directory,
                            execution,
                            reason,
                            None,
                        )?;
                        return Ok(ResolveTurnApprovalResult {
                            transitioned: true,
                            events,
                            last_committed_sequence: execution.position.sequence,
                            awaiting_continuation: None,
                        });
                    }
                    self.complete_and_enter_next(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution,
                        Some(String::from("tool-gate:approval-resumed")),
                    )?;
                    if execution.executor.adapter_kind() == Some(StyleAdapterKind::ResearchLoop) {
                        if execution.current.directive != StyleNodeDirective::PersistArtifact {
                            return Err(RunTurnError::UnexpectedStyleNode {
                                expected: "persist_artifact",
                                actual: execution.current.id.clone(),
                            });
                        }
                        let preflight = persistence
                            .load_session(LoadSessionCommand {
                                session_directory: session_directory.clone(),
                                expected_session_id: session_id,
                            })
                            .map_err(RunTurnError::Persistence)?;
                        let prompt = current_run_user(&preflight.state, &resumed_turn)?
                            .text
                            .clone();
                        let base_cancellation_id =
                            research_base_run_id_from_state(&preflight.state)
                                .ok_or(RunTurnError::InvalidContinuationPayload)?;
                        let research_command = RunTurnCommand {
                            sessions_root: resumed_turn.sessions_root.clone(),
                            session_id: resumed_turn.session_id.clone(),
                            prompt,
                            provider: resumed_turn.provider.clone(),
                            model: resumed_turn.model.clone(),
                            options: resumed_turn.options.clone(),
                            cancellation_id: base_cancellation_id.to_owned(),
                        };
                        let result = self
                            .run_research_loop(
                                research_command,
                                None,
                                None,
                                session_id,
                                session_directory,
                                persistence,
                                preflight,
                            )
                            .await?;
                        return Ok(ResolveTurnApprovalResult {
                            transitioned: true,
                            events: result.events,
                            last_committed_sequence: result.last_committed_sequence,
                            awaiting_continuation: result.awaiting_continuation,
                        });
                    }
                    if execution.current.directive != StyleNodeDirective::CompleteTurn {
                        return Err(RunTurnError::UnexpectedStyleNode {
                            expected: "complete_turn",
                            actual: execution.current.id.clone(),
                        });
                    }
                }
                let assistant_events = Self::assistant_events_for_completion(
                    &persistence,
                    session_id,
                    &session_directory,
                    style_turn.as_ref(),
                    &resumed_turn,
                    &events,
                )?;
                let assistant_position = self.commit_visible_assistant(
                    &persistence,
                    session_id,
                    session_directory.clone(),
                    style_turn
                        .as_ref()
                        .map_or(position.sequence, |value| value.position.sequence),
                    style_turn
                        .as_ref()
                        .map_or(position.event_id, |value| value.position.event_id),
                    &resumed_turn.cancellation_id,
                    &assistant_events,
                )?;
                let assistant_position = if style_turn.as_ref().is_some_and(|execution| {
                    execution.executor.adapter_kind() == Some(StyleAdapterKind::EphemeralTurn)
                }) {
                    self.discard_ephemeral_projection(
                        &persistence,
                        session_id,
                        &session_directory,
                        assistant_position,
                        &resumed_turn,
                    )
                    .await?
                } else {
                    assistant_position
                };
                let last_committed_position = if let Some(execution) = style_turn.as_mut() {
                    execution.position = assistant_position;
                    self.complete_terminal_style_node(
                        &persistence,
                        session_id,
                        &session_directory,
                        execution,
                        Some(format!("turn:{}", resumed_turn.cancellation_id)),
                    )?;
                    execution.position
                } else {
                    assistant_position
                };
                Ok(ResolveTurnApprovalResult {
                    transitioned: true,
                    events,
                    last_committed_sequence: last_committed_position.sequence,
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
        + MemoryDataPort
        + ArtifactDataPort
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
        if !wake.transitioned && !command.allow_resumed_recovery {
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

impl<D> ScheduledRecoveryLogicPort for TurnLogic<D>
where
    D: Clone
        + Send
        + Sync
        + EventIdentityDataPort
        + JournalEventDataPort
        + HarnessDataPort
        + ContinuationDataPort
        + MemoryDataPort
        + agentmod_runtime_data::tool::ToolDataPort,
{
    fn record_scheduled_recovery(
        &self,
        command: RecordScheduledRecoveryCommand,
    ) -> Result<Sequence, RunTurnError> {
        if command.execution_id.len() != 64
            || !command
                .execution_id
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
            || command.schedule_id.trim().is_empty()
            || !matches!(
                command.outcome.as_str(),
                "succeeded" | "failed" | "indeterminate_failed" | "awaiting_approval"
            )
        {
            return Err(RunTurnError::Invalid);
        }
        let session_id =
            SessionId::from_str(&command.session_id).map_err(|_| RunTurnError::InvalidSession)?;
        let session_directory = command.sessions_root.join(session_id.to_string());
        let persistence = SessionPersistenceLogic::new(self.data.clone());
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.clone(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        self.commit_next(
            &persistence,
            session_id,
            &session_directory,
            loaded.state.last_sequence,
            loaded.last_event_id,
            RuntimeCommittedEvent::SchedulerDeliveryReconciled(SchedulerDeliveryReconciledEvent {
                execution_id: command.execution_id,
                schedule_id: command.schedule_id,
                outcome: command.outcome,
                continuation_id: command.continuation_id,
            }),
        )
        .map(|(sequence, _)| sequence)
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
        + MemoryDataPort
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
            if execution.execution_id.as_deref() != Some(receipt.execution_id.as_str()) {
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
            if execution.action_digest != Some(digest)
                || execution.execution_id.as_deref() != Some(authorized.original.id.0.as_str())
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
        + MemoryDataPort
        + agentmod_runtime_data::tool::ToolDataPort,
{
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "context composition keeps proposal authorization, bounded retrieval, canonical replacement, and compaction ordering explicit"
    )]
    async fn compose_style_context(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut state: crate::session::SessionState,
        mut position: JournalPosition,
        command: &RunTurnCommand,
        boundary: ContextCompositionBoundary,
        origin: ContextCompositionOrigin,
    ) -> Result<(crate::session::SessionState, JournalPosition), RunTurnError> {
        let binding = state
            .style_binding
            .clone()
            .ok_or(RunTurnError::StyleMigrationRequired)?;
        let adapter_kind = CompiledStyleExecutor::from_binding(&binding)
            .map_err(RunTurnError::StyleExecutor)?
            .adapter_kind()
            .ok_or_else(|| RunTurnError::UnsupportedStyleExecution(binding.id.clone()))?;
        let fresh_context_kind = match adapter_kind {
            StyleAdapterKind::EphemeralTurn => {
                Some(("ephemeral-fresh-context", "ephemeral_fresh_context"))
            }
            StyleAdapterKind::ResearchLoop => {
                Some(("research-fresh-context", "research_fresh_context"))
            }
            StyleAdapterKind::PersistentTurn
            | StyleAdapterKind::PlannerWorkerReviewer
            | StyleAdapterKind::DeclarativeGraph => None,
        };
        let fresh_isolated_context = fresh_context_kind.is_some()
            && boundary == ContextCompositionBoundary::TurnStart
            && matches!(
                origin,
                ContextCompositionOrigin::UserTurn | ContextCompositionOrigin::ChildTask
            );
        let (next_state, next_position, boundary_identity, completed_phases, already_completed) =
            self.begin_or_resume_context_boundary(
                persistence,
                session_id,
                session_directory,
                &state,
                position,
                command,
                boundary,
                origin,
            )?;
        state = next_state;
        position = next_position;
        if already_completed {
            return Ok((state, position));
        }
        let timing = binding.memory.retrieval_timing.as_str();
        let retrieve_now = matches!(
            (timing, boundary),
            (
                "turnstart" | "turn_start" | "iterationstart" | "iteration_start",
                ContextCompositionBoundary::TurnStart
            ) | (
                "beforemodelrequest" | "before_model_request",
                ContextCompositionBoundary::BeforeModelRequest
            )
        );
        let normalize_memory = boundary == ContextCompositionBoundary::TurnStart || retrieve_now;
        if !completed_phases.iter().any(|phase| phase == "memory") {
            if context_phase_started(&state, &boundary_identity, "memory") {
                return Err(RunTurnError::AmbiguousContextPhase(String::from("memory")));
            }
            let memory_phase = ContextPhaseIdentity {
                boundary: boundary_identity.clone(),
                phase: String::from("memory"),
            };
            position = self.commit_context_phase_started(
                persistence,
                session_id,
                session_directory,
                position,
                memory_phase.clone(),
            )?;
            state = Self::load_state(persistence, session_id, session_directory)?;
            let mut replacement = if fresh_isolated_context {
                vec![current_provider_input_entry(&state, command)?]
            } else {
                projection_without_retrieved_memory(state.conversation.provider_projection())
            };
            let should_retrieve = binding.memory.provider != "none"
                && retrieve_now
                && binding.memory.injection_location != "none";
            let fresh_source_sequence = fresh_isolated_context
                .then(|| current_run_user_sequence(&state, command))
                .transpose()?;
            let mut reserved_identity = None;
            let injection_sequence = position
                .sequence
                .checked_next()
                .map_err(|_| RunTurnError::SequenceOverflow)?;
            if normalize_memory && should_retrieve {
                let identity = self
                    .data
                    .allocate_event_identity(AllocateEventIdentityDataRequest)
                    .map_err(RunTurnError::Identity)?;
                reserved_identity = Some(identity);
                if matches!(
                    binding.memory.injection_location.as_str(),
                    "contextartifact" | "context_artifact"
                ) {
                    return Err(RunTurnError::MemoryContextArtifactRequired);
                }
                let query = construct_memory_query(&binding, &state, command)?;
                self.authorize_style_action(
                    ActionProposal {
                        id: ProposalId(format!("context-memory:{}", command.cancellation_id)),
                        action: ConsequentialAction::ContextConstruction {
                            strategy: format!("memory:{}", binding.memory.provider),
                        },
                        style: binding.id.clone(),
                        workspace: state.workspace.clone(),
                        origin: String::from("runtime"),
                    },
                    "memory retrieval",
                )
                .await?;
                let memory = MemoryLogic::new(self.data.clone());
                let mut retrieved_entries = Vec::new();
                let mut remaining_items = usize::try_from(binding.memory.max_items)
                    .map_err(|_| RunTurnError::MemoryBoundOverflow)?;
                let mut remaining_bytes = binding.memory.max_injected_bytes;
                for scope in &binding.memory.scopes {
                    if remaining_items == 0 || remaining_bytes == 0 {
                        break;
                    }
                    let scope = memory_scope(scope, session_id, &state.workspace)?;
                    let items = memory
                        .retrieve_memory(RetrieveMemoryCommand {
                            provider: binding.memory.provider.clone(),
                            scope,
                            query: query.clone(),
                            limit: remaining_items,
                            injection_event: identity.event_id,
                        })
                        .map_err(RunTurnError::Memory)?;
                    for item in items {
                        if remaining_items == 0 {
                            break;
                        }
                        let entry = ConversationEntry::RetrievedMemory(RetrievedMemoryEntry {
                            id: ConversationEntryId(format!(
                                "memory:{}:{}:{}",
                                binding.memory.provider,
                                injection_sequence.get(),
                                item.reference
                            )),
                            provider: item.provider,
                            query: item.query,
                            scope: item.scope,
                            source: item.source,
                            reference: item.reference,
                            score: item.score,
                            content: item.content,
                            injection_sequence,
                            injection_event: Some(item.injection_event),
                            created_at_millis: item.created_at.get(),
                            size_bytes: item.size.get(),
                        });
                        let (entry, contribution) = memory_entry_with_serialized_size(entry)?;
                        if contribution > remaining_bytes {
                            continue;
                        }
                        remaining_bytes = remaining_bytes
                            .checked_sub(contribution)
                            .ok_or(RunTurnError::MemoryBoundOverflow)?;
                        remaining_items = remaining_items
                            .checked_sub(1)
                            .ok_or(RunTurnError::MemoryBoundOverflow)?;
                        retrieved_entries.push(entry);
                    }
                }
                inject_memory(
                    &mut replacement,
                    retrieved_entries,
                    &binding.memory.injection_location,
                )?;
            }
            if replacement == state.conversation.provider_projection() && !fresh_isolated_context {
                position = self.commit_context_phase_completed(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    memory_phase,
                )?;
            } else {
                let identity = match reserved_identity {
                    Some(identity) => identity,
                    None => self
                        .data
                        .allocate_event_identity(AllocateEventIdentityDataRequest)
                        .map_err(RunTurnError::Identity)?,
                };
                let provenance = ProjectionProvenance {
                    projection_id: format!(
                        "{}:{}:{}",
                        if fresh_isolated_context {
                            fresh_context_kind
                                .map(|(projection_prefix, _)| projection_prefix)
                                .expect("fresh context kind exists")
                        } else {
                            "memory"
                        },
                        command.cancellation_id,
                        injection_sequence.get()
                    ),
                    source_range: fresh_source_sequence.map(|sequence| (sequence, sequence)),
                    method: if fresh_isolated_context {
                        fresh_context_kind
                            .map(|(_, method)| method.to_owned())
                            .expect("fresh context kind exists")
                    } else {
                        format!("memory:{}", binding.memory.provider)
                    },
                    committed_at: injection_sequence,
                    artifact_id: None,
                };
                self.authorize_context_replacement(
                    &binding.id,
                    &state.workspace,
                    &command.cancellation_id,
                    if fresh_isolated_context {
                        "fresh_context"
                    } else {
                        "memory"
                    },
                    &replacement,
                )
                .await?;
                position = Self::commit_context_replacement_with_identity(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    identity,
                    replacement,
                    provenance,
                    Some(memory_phase),
                )?;
            }
            state = Self::load_state(persistence, session_id, session_directory)?;
        }

        if boundary != ContextCompositionBoundary::BeforeModelRequest {
            return self.complete_context_boundary(
                persistence,
                session_id,
                session_directory,
                &state,
                position,
                boundary_identity,
            );
        }
        let projection_measure = measure_projection(state.conversation.provider_projection())
            .map_err(map_projection_measure_error)?;
        let projection_limit = effective_projection_limit(&binding.compaction)?;
        let pressure_triggered = binding
            .compaction
            .trigger_tokens
            .is_some_and(|trigger| projection_measure.estimated_tokens >= trigger);
        let over_limit =
            projection_limit.is_some_and(|limit| projection_measure.estimated_tokens > limit);
        let over_byte_cap = projection_measure.serialized_bytes > MAX_PROVIDER_PROJECTION_BYTES;
        if !completed_phases.iter().any(|phase| phase == "compaction") {
            if context_phase_started(&state, &boundary_identity, "compaction") {
                return Err(RunTurnError::AmbiguousContextPhase(String::from(
                    "compaction",
                )));
            }
            let phase = ContextPhaseIdentity {
                boundary: boundary_identity.clone(),
                phase: String::from("compaction"),
            };
            position = self.commit_context_phase_started(
                persistence,
                session_id,
                session_directory,
                position,
                phase.clone(),
            )?;
            state = Self::load_state(persistence, session_id, session_directory)?;
            if pressure_triggered || over_limit || over_byte_cap {
                if binding.compaction.strategy == "none" {
                    if over_byte_cap {
                        return Err(RunTurnError::ProviderProjectionByteLimitExceeded {
                            serialized_bytes: projection_measure.serialized_bytes,
                            limit: MAX_PROVIDER_PROJECTION_BYTES,
                        });
                    }
                    return Err(RunTurnError::ProviderProjectionLimitExceeded {
                        estimated_tokens: projection_measure.estimated_tokens,
                        limit: projection_limit.unwrap_or(0),
                    });
                }
                self.authorize_style_action(
                    ActionProposal {
                        id: ProposalId(format!("compaction:{}", command.cancellation_id)),
                        action: ConsequentialAction::Compaction {
                            strategy: binding.compaction.strategy.clone(),
                        },
                        style: binding.id.clone(),
                        workspace: state.workspace.clone(),
                        origin: String::from("runtime"),
                    },
                    "compaction",
                )
                .await?;
                let committed_at = position
                    .sequence
                    .checked_next()
                    .map_err(|_| RunTurnError::SequenceOverflow)?;
                let context = CompactionContext {
                    projection_id: format!(
                        "compaction:{}:{}",
                        command.cancellation_id,
                        committed_at.get()
                    ),
                    committed_at,
                };
                let mut plan = match binding.compaction.strategy.as_str() {
                    "sliding_window" => compact_sliding_window_to_bound(
                        &state.conversation,
                        &binding.compaction.preservation_requirements,
                        projection_limit,
                        &context,
                    )?,
                    "tool_output_eviction" => compact_projection(
                        &state.conversation,
                        CompactionStrategy::ToolOutputEviction {
                            max_visible_bytes: MAX_TOOL_RESULT_BYTES,
                        },
                        context,
                    )
                    .map_err(RunTurnError::Compaction)?,
                    "summary" => return Err(RunTurnError::ApprovedSummaryRequired),
                    "artifact_handoff" => {
                        return Err(RunTurnError::ApprovedArtifactHandoffRequired);
                    }
                    _ => {
                        return Err(RunTurnError::UnsupportedCompactionStrategy(
                            binding.compaction.strategy,
                        ));
                    }
                };
                validate_projection_preservation(
                    state.conversation.provider_projection(),
                    &plan.replacement,
                    &binding.compaction.preservation_requirements,
                )?;
                validate_projection_measure(&plan.replacement, projection_limit)?;
                self.authorize_context_replacement(
                    &binding.id,
                    &state.workspace,
                    &command.cancellation_id,
                    "compaction",
                    &plan.replacement,
                )
                .await?;
                plan.provenance.committed_at = committed_at;
                let (sequence, event_id) = self.commit_next(
                    persistence,
                    session_id,
                    session_directory,
                    position.sequence,
                    position.event_id,
                    RuntimeCommittedEvent::ContextProjectionReplaced(
                        ContextProjectionReplacedEvent {
                            replacement: plan.replacement,
                            provenance: plan.provenance,
                            context_phase: Some(phase),
                        },
                    ),
                )?;
                position = JournalPosition { sequence, event_id };
            } else {
                position = self.commit_context_phase_completed(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    phase,
                )?;
            }
            state = Self::load_state(persistence, session_id, session_directory)?;
        }
        self.complete_context_boundary(
            persistence,
            session_id,
            session_directory,
            &state,
            position,
            boundary_identity,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "boundary recovery binds the exact session, journal head, run command, and lifecycle boundary"
    )]
    fn begin_or_resume_context_boundary(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        state: &crate::session::SessionState,
        position: JournalPosition,
        command: &RunTurnCommand,
        boundary: ContextCompositionBoundary,
        origin: ContextCompositionOrigin,
    ) -> Result<
        (
            crate::session::SessionState,
            JournalPosition,
            ContextBoundaryIdentity,
            Vec<String>,
            bool,
        ),
        RunTurnError,
    > {
        let execution = state
            .style_execution
            .as_ref()
            .ok_or(RunTurnError::StyleGraphMismatch)?;
        let active = execution
            .active_node
            .as_ref()
            .ok_or(RunTurnError::StyleGraphMismatch)?;
        let boundary_name = match boundary {
            ContextCompositionBoundary::TurnStart => "turn_start",
            ContextCompositionBoundary::BeforeModelRequest => "before_model_request",
            ContextCompositionBoundary::BeforeTurnCompletion => "before_turn_completion",
        };
        if let Some(existing) = execution.context_boundaries.iter().rev().find(|candidate| {
            candidate.identity.node_id == active.node_id
                && candidate.identity.boundary == boundary_name
                && candidate.identity.run_id == command.cancellation_id
                && candidate.identity.origin == boundary_origin(origin)
                && current_context_request_hash(state, command)
                    .is_ok_and(|hash| hash == candidate.identity.request_hash)
                && candidate.last_sequence == position.sequence
        }) {
            let identity = existing.identity.clone();
            let completed_phases = existing.completed_phases.clone();
            let completed = existing.completed_at.is_some();
            return Ok((
                state.clone(),
                position,
                identity,
                completed_phases,
                completed,
            ));
        }
        let request_hash = current_context_request_hash(state, command)?;
        let identity = ContextBoundaryIdentity {
            node_id: active.node_id.clone(),
            boundary: boundary_name.into(),
            run_id: command.cancellation_id.clone(),
            origin: boundary_origin(origin),
            request_hash,
            source_head: position.sequence,
        };
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                identity: identity.clone(),
            }),
        )?;
        let position = JournalPosition { sequence, event_id };
        let state = Self::load_state(persistence, session_id, session_directory)?;
        Ok((state, position, identity, Vec::new(), false))
    }

    fn commit_context_phase_completed(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        position: JournalPosition,
        identity: ContextPhaseIdentity,
    ) -> Result<JournalPosition, RunTurnError> {
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent { identity }),
        )?;
        Ok(JournalPosition { sequence, event_id })
    }

    fn commit_context_phase_started(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        position: JournalPosition,
        identity: ContextPhaseIdentity,
    ) -> Result<JournalPosition, RunTurnError> {
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent { identity }),
        )?;
        Ok(JournalPosition { sequence, event_id })
    }

    fn complete_context_boundary(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        state: &crate::session::SessionState,
        position: JournalPosition,
        identity: ContextBoundaryIdentity,
    ) -> Result<(crate::session::SessionState, JournalPosition), RunTurnError> {
        let limit = effective_projection_limit(
            &state
                .style_binding
                .as_ref()
                .ok_or(RunTurnError::StyleMigrationRequired)?
                .compaction,
        )?;
        let measure = if identity.boundary == "before_model_request" {
            validate_projection_measure(state.conversation.provider_projection(), limit)?
        } else {
            measure_projection(state.conversation.provider_projection())
                .map_err(map_projection_measure_error)?
        };
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::ContextBoundaryCompleted(ContextBoundaryCompletedEvent {
                identity,
                projection_hash: measure.projection_hash,
                estimated_tokens: measure.estimated_tokens,
                serialized_bytes: measure.serialized_bytes,
            }),
        )?;
        let position = JournalPosition { sequence, event_id };
        let state = Self::load_state(persistence, session_id, session_directory)?;
        Ok((state, position))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fail-closed discard boundary keeps phase evidence, authorization, replacement, and boundary completion adjacent"
    )]
    async fn discard_ephemeral_projection(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        position: JournalPosition,
        command: &RunTurnCommand,
    ) -> Result<JournalPosition, RunTurnError> {
        let mut state = Self::load_state(persistence, session_id, session_directory)?;
        let binding = state
            .style_binding
            .clone()
            .ok_or(RunTurnError::StyleMigrationRequired)?;
        if CompiledStyleExecutor::from_binding(&binding)
            .map_err(RunTurnError::StyleExecutor)?
            .adapter_kind()
            != Some(StyleAdapterKind::EphemeralTurn)
        {
            return Err(RunTurnError::UnsupportedStyleExecution(binding.id));
        }
        let (next_state, mut position, boundary_identity, completed_phases, already_completed) =
            self.begin_or_resume_context_boundary(
                persistence,
                session_id,
                session_directory,
                &state,
                position,
                command,
                ContextCompositionBoundary::BeforeTurnCompletion,
                if state.child_origin.is_some() {
                    ContextCompositionOrigin::ChildTask
                } else {
                    ContextCompositionOrigin::UserTurn
                },
            )?;
        state = next_state;
        if already_completed {
            return Ok(position);
        }
        if completed_phases.iter().any(|phase| phase == "discard") {
            return self
                .complete_context_boundary(
                    persistence,
                    session_id,
                    session_directory,
                    &state,
                    position,
                    boundary_identity,
                )
                .map(|(_, position)| position);
        }
        if context_phase_started(&state, &boundary_identity, "discard") {
            return Err(RunTurnError::AmbiguousContextPhase(String::from("discard")));
        }
        let phase = ContextPhaseIdentity {
            boundary: boundary_identity.clone(),
            phase: String::from("discard"),
        };
        position = self.commit_context_phase_started(
            persistence,
            session_id,
            session_directory,
            position,
            phase.clone(),
        )?;
        state = Self::load_state(persistence, session_id, session_directory)?;
        let replacement = Vec::new();
        self.authorize_context_replacement(
            &binding.id,
            &state.workspace,
            &command.cancellation_id,
            "ephemeral_turn_discard",
            &replacement,
        )
        .await?;
        let committed_at = position
            .sequence
            .checked_next()
            .map_err(|_| RunTurnError::SequenceOverflow)?;
        let identity = self
            .data
            .allocate_event_identity(AllocateEventIdentityDataRequest)
            .map_err(RunTurnError::Identity)?;
        position = Self::commit_context_replacement_with_identity(
            persistence,
            session_id,
            session_directory,
            position,
            identity,
            replacement,
            ProjectionProvenance {
                projection_id: format!(
                    "ephemeral-turn-discard:{}:{}",
                    command.cancellation_id,
                    committed_at.get()
                ),
                source_range: None,
                method: String::from("ephemeral_discard"),
                committed_at,
                artifact_id: None,
            },
            Some(phase),
        )?;
        state = Self::load_state(persistence, session_id, session_directory)?;
        self.complete_context_boundary(
            persistence,
            session_id,
            session_directory,
            &state,
            position,
            boundary_identity,
        )
        .map(|(_, position)| position)
    }

    async fn authorize_context_replacement(
        &self,
        style: &str,
        workspace: &str,
        cancellation_id: &str,
        phase: &str,
        replacement: &[ConversationEntry],
    ) -> Result<(), RunTurnError> {
        let bytes = serde_json::to_vec(replacement).map_err(|_| RunTurnError::Event)?;
        self.authorize_style_action(
            ActionProposal {
                id: ProposalId(format!("context-replacement:{phase}:{cancellation_id}")),
                action: ConsequentialAction::ContextReplacement {
                    projection_hash: ContentHash::digest(&bytes),
                },
                style: style.to_owned(),
                workspace: workspace.to_owned(),
                origin: String::from("runtime"),
            },
            "context replacement",
        )
        .await
    }

    async fn authorize_style_action(
        &self,
        proposal: ActionProposal,
        operation: &'static str,
    ) -> Result<(), RunTurnError> {
        let result = intercept_action(
            proposal.clone(),
            &self.policy.style_pipeline,
            &self.policy.plugin_pipeline,
            ActionCapabilities::all(),
            &self.policy.user_policy,
            &self.policy.mandatory_policy,
        )
        .await;
        match result.outcome {
            InterceptionOutcome::Approved { executable, .. } if executable == proposal => Ok(()),
            InterceptionOutcome::Approved { .. } => {
                Err(RunTurnError::InvalidContextInterceptionReplacement)
            }
            InterceptionOutcome::RequireApproval { reason, .. } => {
                Err(RunTurnError::ContextApprovalRequired { operation, reason })
            }
            InterceptionOutcome::Rejected { reason }
            | InterceptionOutcome::Cancelled { reason } => {
                Err(RunTurnError::ContextRejected { operation, reason })
            }
            InterceptionOutcome::Deferred { .. }
            | InterceptionOutcome::Forked { .. }
            | InterceptionOutcome::Aborted { .. } => {
                Err(RunTurnError::UnsupportedContextDecision(operation))
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the commit binds exact session journal position, reserved identity, replacement, and provenance"
    )]
    fn commit_context_replacement_with_identity(
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        previous: JournalPosition,
        identity: EventIdentityDataRecord,
        replacement: Vec<ConversationEntry>,
        provenance: ProjectionProvenance,
        context_phase: Option<ContextPhaseIdentity>,
    ) -> Result<JournalPosition, RunTurnError> {
        let event = Self::seal_event_with_identity(
            session_id,
            provenance.committed_at,
            Some(CausationId::from_uuid(previous.event_id.into_uuid())),
            identity,
            RuntimeCommittedEvent::ContextProjectionReplaced(ContextProjectionReplacedEvent {
                replacement,
                provenance,
                context_phase,
            }),
        )?;
        let position = JournalPosition {
            sequence: event.metadata.sequence,
            event_id: event.metadata.event_id,
        };
        persistence
            .commit_event(CommitSessionEventCommand {
                session_directory: session_directory.to_owned(),
                event,
                durability: CommitDurability::Data,
            })
            .map_err(RunTurnError::Persistence)?;
        Ok(position)
    }

    fn load_state(
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
    ) -> Result<crate::session::SessionState, RunTurnError> {
        persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map(|loaded| loaded.state)
            .map_err(RunTurnError::Persistence)
    }

    fn commit_plugin_invocations(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut position: JournalPosition,
        state: &crate::session::SessionState,
        audit: &[InterceptorAuditStep],
    ) -> Result<JournalPosition, RunTurnError> {
        if !audit
            .iter()
            .any(|step| step.scope == InterceptorScope::Plugin)
        {
            return Ok(position);
        }
        let binding = state
            .style_binding
            .as_ref()
            .ok_or(RunTurnError::StyleMigrationRequired)?;
        let compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&binding.compiled_style_json)
                .map_err(|_| RunTurnError::StyleBindingInvalid)?;
        for step in audit
            .iter()
            .filter(|step| step.scope == InterceptorScope::Plugin)
        {
            let plugin_id = compiled
                .interceptors
                .iter()
                .find(|declaration| declaration.id == step.handler)
                .map(|declaration| declaration.owner.clone())
                .ok_or(RunTurnError::StyleBindingInvalid)?;
            let input_digest = step.input.digest().map_err(|_| RunTurnError::Event)?;
            let (outcome, output_digest) = match &step.result {
                InterceptorAuditResult::Continue { output, replaced } => (
                    if *replaced { "replace" } else { "continue" },
                    Some(output.digest().map_err(|_| RunTurnError::Event)?),
                ),
                InterceptorAuditResult::Reject { .. } => ("reject", None),
                InterceptorAuditResult::RequireApproval { .. } => ("require_approval", None),
                InterceptorAuditResult::Defer { .. } => ("defer", None),
                InterceptorAuditResult::Cancel { .. } => ("cancel", None),
                InterceptorAuditResult::Fork { .. } => ("fork", None),
                InterceptorAuditResult::Failure { .. } => ("failure", None),
            };
            (position.sequence, position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                position.sequence,
                position.event_id,
                RuntimeCommittedEvent::PluginInvocationCompleted(PluginInvocationCompletedEvent {
                    plugin_id,
                    handler: step.handler.clone(),
                    action_kind: step.input.action.kind().to_owned(),
                    proposal_id: step.input.id.0.clone(),
                    input_digest,
                    output_digest,
                    outcome: outcome.to_owned(),
                }),
            )?;
        }
        Ok(position)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "authorization keeps activation, proposal, plugin audit, and approval commits adjacent"
    )]
    async fn authorize_and_commit(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut position: JournalPosition,
        state: crate::session::SessionState,
        command: &RunTurnCommand,
    ) -> Result<AuthorizedTurn, RunTurnError> {
        let session_policy = self
            .policy_for_state(&state, &command.cancellation_id)
            .await?;
        if !session_policy.activated_plugin_ids.is_empty()
            && state.plugins.activated_plugin_ids != session_policy.activated_plugin_ids
        {
            let plugin_set_hash = state
                .style_binding
                .as_ref()
                .ok_or(RunTurnError::StyleMigrationRequired)?
                .plugin_set_hash;
            (position.sequence, position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                position.sequence,
                position.event_id,
                RuntimeCommittedEvent::PluginSetActivated(PluginSetActivatedEvent {
                    plugin_ids: session_policy.activated_plugin_ids.clone(),
                    plugin_set_hash,
                }),
            )?;
        }
        let provider = ProviderExecutionLogic::new(self.data.clone(), session_policy.execution);
        let harness = state
            .style_binding
            .as_ref()
            .map_or_else(|| String::from("native"), |binding| binding.harness.clone());
        let prepared = provider
            .prepare(ExecuteProviderCommand {
                harness,
                session_id: command.session_id.clone(),
                provider: command.provider.clone(),
                model: command.model.clone(),
                entries: project(state.conversation.provider_projection()),
                options: command.options.clone(),
                cancellation_id: command.cancellation_id.clone(),
                style: state.style.clone(),
                workspace: state.workspace.clone(),
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
                harness: original.harness.clone(),
                provider: original.provider.clone(),
                model: original.model.clone(),
                projection_hash: original.projection_hash,
            }),
        )?;
        let request = match provider.authorize_prepared(prepared).await {
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
        let invocation_position = self.commit_plugin_invocations(
            persistence,
            session_id,
            session_directory,
            JournalPosition { sequence, event_id },
            &state,
            &request.interceptor_audit,
        )?;
        let action_digest = request
            .executable
            .digest()
            .map_err(|_| RunTurnError::Event)?;
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            invocation_position.sequence,
            invocation_position.event_id,
            RuntimeCommittedEvent::ModelRequestApproved(ModelRequestApprovedEvent {
                proposal_id: request.original.id.0.clone(),
                harness: executable.harness.clone(),
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
            let mut loaded = persistence
                .load_session(LoadSessionCommand {
                    session_directory: session_directory.to_owned(),
                    expected_session_id: session_id,
                })
                .map_err(RunTurnError::Persistence)?;
            if loaded.state.style_binding.is_some() {
                let composed = self
                    .compose_style_context(
                        persistence,
                        session_id,
                        session_directory,
                        loaded.state,
                        position,
                        command,
                        ContextCompositionBoundary::BeforeModelRequest,
                        ContextCompositionOrigin::ToolContinuation,
                    )
                    .await;
                let (state, composed_position) = match composed {
                    Ok(value) => value,
                    Err(error) => {
                        self.fail_active_bound_style_at_head(
                            persistence,
                            session_id,
                            session_directory,
                            "context_composition_failed",
                        )?;
                        return Err(error);
                    }
                };
                loaded.state = state;
                position = composed_position;
            }
            let stream = self
                .provider
                .continue_execution_stream(
                    loaded
                        .state
                        .style_binding
                        .as_ref()
                        .ok_or(RunTurnError::StyleMigrationRequired)?
                        .harness
                        .clone(),
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
        let tools = ToolExecutionLogic::new(
            self.data.clone(),
            ToolExecutionPolicy {
                style_pipeline: self.policy.style_pipeline.clone(),
                plugin_pipeline: self
                    .policy_for_state(&state, &command.cancellation_id)
                    .await?
                    .execution
                    .plugin_pipeline,
                user_policy: self.policy.user_policy.clone(),
                mandatory_policy: self.policy.mandatory_policy.clone(),
            },
        );
        let prepared = tools
            .prepare(PrepareToolCommand {
                session_id: command.session_id.clone(),
                workspace: PathBuf::from(state.workspace.clone()),
                call_id: call_id.to_owned(),
                tool: tool.to_owned(),
                arguments,
                cancellation_id: command.cancellation_id.clone(),
                style: state.style.clone(),
            })
            .map_err(RunTurnError::Tool)?;
        let ConsequentialAction::ToolCall(original_action) = &prepared.original.action else {
            return Err(RunTurnError::Tool(ToolExecutionError::InvalidReplacement));
        };
        let original_action = original_action.clone();
        let original_action_digest = prepared
            .original
            .digest()
            .map_err(|_| RunTurnError::Event)?;
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
        let authorized = match tools.authorize_prepared_outcome(prepared).await {
            Ok(ToolAuthorizationOutcome::Authorized(authorized)) => authorized,
            Ok(ToolAuthorizationOutcome::ApprovalRequired { pending, reason }) => {
                position = self.commit_plugin_invocations(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    &state,
                    &pending.interceptor_audit,
                )?;
                if harness_continuation == "style-owned" {
                    return Err(RunTurnError::StyleOwnedToolApprovalUnsupported);
                }
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
                    Some(original_action_digest),
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
        position = self.commit_plugin_invocations(
            persistence,
            session_id,
            session_directory,
            position,
            &state,
            &authorized.interceptor_audit,
        )?;
        if harness_continuation == "style-owned"
            && authorized
                .executable
                .digest()
                .map_err(|_| RunTurnError::Event)?
                != original_action_digest
        {
            return Err(RunTurnError::StyleOwnedToolReplacementUnsupported);
        }
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
                    None,
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
                        action_digest: None,
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
                        action_digest: None,
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
        action_digest: Option<ContentHash>,
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
                action_digest,
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
        self.commit_tool_result_conversation(
            persistence,
            session_id,
            session_directory,
            position,
            call_id,
            result,
            artifact,
            truncated,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_tool_result_conversation(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut position: JournalPosition,
        call_id: &str,
        result: &Value,
        artifact: Option<&str>,
        truncated: bool,
    ) -> Result<JournalPosition, RunTurnError> {
        let result_sequence = position
            .sequence
            .checked_next()
            .map_err(|_| RunTurnError::SequenceOverflow)?;
        let (content, artifact_id, result_truncated) =
            project_tool_result(result, artifact, truncated)?;
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
                    truncated: result_truncated,
                    source_sequence: result_sequence,
                }),
            }),
        )?;
        Ok(position)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "repair binds the exact canonical session head, action, and terminal receipt"
    )]
    fn repair_terminal_tool_conversation(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        state: &crate::session::SessionState,
        position: JournalPosition,
        call_id: &str,
        action: &crate::action::ToolCallAction,
        expected_action_digest: ContentHash,
        terminal: &crate::session::ToolExecutionRecord,
    ) -> Result<JournalPosition, RunTurnError> {
        if terminal.action_digest != Some(expected_action_digest) {
            return Err(RunTurnError::InvalidRecoveryReceipt(call_id.to_owned()));
        }
        let calls = state
            .conversation
            .history()
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| match entry {
                ConversationEntry::ToolCallRequest(call) if call.call_id == call_id => {
                    Some((index, call))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let results = state
            .conversation
            .history()
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| match entry {
                ConversationEntry::ToolResult(result) if result.call_id == call_id => {
                    Some((index, result))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let (result, artifact, truncated) = terminal_projection(terminal, call_id)?;
        let (content, artifact_id, result_truncated) =
            project_tool_result(&result, artifact.as_deref(), truncated)?;
        match (calls.as_slice(), results.as_slice()) {
            ([], []) => self.commit_tool_conversation(
                persistence,
                session_id,
                session_directory,
                position,
                call_id,
                action,
                &result,
                artifact.as_deref(),
                truncated,
            ),
            ([(call_index, call)], [(result_index, tool_result)])
                if call.tool == action.tool
                    && call.arguments == action.arguments
                    && call_index < result_index
                    && call.source_sequence < tool_result.source_sequence
                    && tool_result.content == content
                    && tool_result.artifact_id == artifact_id
                    && tool_result.truncated == result_truncated =>
            {
                Ok(position)
            }
            ([(_, call)], []) if call.tool == action.tool && call.arguments == action.arguments => {
                self.commit_tool_result_conversation(
                    persistence,
                    session_id,
                    session_directory,
                    position,
                    call_id,
                    &result,
                    artifact.as_deref(),
                    truncated,
                )
            }
            _ => Err(RunTurnError::ToolConversationRecoveryConflict(
                call_id.to_owned(),
            )),
        }
    }

    fn begin_style_turn(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        state: &crate::session::SessionState,
        position: JournalPosition,
    ) -> Result<ActiveStyleTurn, RunTurnError> {
        self.begin_style_turn_with_input(
            persistence,
            session_id,
            session_directory,
            state,
            position,
            None,
        )
    }

    fn begin_style_turn_with_input(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        state: &crate::session::SessionState,
        mut position: JournalPosition,
        input_reference: Option<String>,
    ) -> Result<ActiveStyleTurn, RunTurnError> {
        let binding = state
            .style_binding
            .as_ref()
            .ok_or(RunTurnError::StyleMigrationRequired)?;
        let executor =
            CompiledStyleExecutor::from_binding(binding).map_err(RunTurnError::StyleExecutor)?;
        let step = next_style_step(state);
        if let Some(execution) = &state.style_execution {
            if execution.graph.as_ref() != &executor.compiled().graph {
                return Err(RunTurnError::StyleGraphMismatch);
            }
            if execution.input_reference != input_reference {
                return Err(RunTurnError::ContextRecoveryIdentityMismatch);
            }
            if let Some(active) = &execution.active_node {
                return Err(RunTurnError::StyleRecoveryRequired(active.node_id.clone()));
            }
            if execution.termination_reason.is_some() {
                return Err(RunTurnError::StyleExecutionTerminal);
            }
        } else {
            (position.sequence, position.event_id) = self.commit_next(
                persistence,
                session_id,
                session_directory,
                position.sequence,
                position.event_id,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(executor.compiled().graph.clone()),
                        input_reference,
                    },
                )),
            )?;
        }
        let current = executor.entry().map_err(RunTurnError::StyleExecutor)?;
        let expected_entry = match executor.adapter_kind() {
            Some(StyleAdapterKind::PersistentTurn | StyleAdapterKind::PlannerWorkerReviewer) => {
                StyleNodeDirective::ModelCall
            }
            Some(StyleAdapterKind::EphemeralTurn | StyleAdapterKind::ResearchLoop) => {
                StyleNodeDirective::ContextTransform
            }
            Some(StyleAdapterKind::DeclarativeGraph) => StyleNodeDirective::ConditionalBranch,
            None => {
                return Err(RunTurnError::UnsupportedStyleExecution(binding.id.clone()));
            }
        };
        if current.directive != expected_entry {
            return Err(RunTurnError::UnexpectedStyleNode {
                expected: match expected_entry {
                    StyleNodeDirective::ModelCall => "model_call",
                    StyleNodeDirective::ContextTransform => "context_transform",
                    StyleNodeDirective::ConditionalBranch => "conditional_branch",
                    _ => unreachable!("supported entry adapter is exhaustive"),
                },
                actual: current.id,
            });
        }
        let max_steps = binding
            .budgets
            .max_steps
            .min(executor.compiled().graph.budget.max_steps);
        if step > max_steps {
            self.commit_style_budget_termination(
                persistence,
                session_id,
                session_directory,
                position,
                &current.id,
                step,
                max_steps,
            )?;
            return Err(RunTurnError::StyleStepBudgetExceeded { limit: max_steps });
        }
        (position.sequence, position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                node_id: current.id.clone(),
                attempt: 1,
                loop_iteration: 0,
                step,
            }),
        )?;
        Ok(ActiveStyleTurn {
            executor,
            current,
            position,
            attempt: 1,
            loop_iteration: 0,
            step,
            max_steps,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "control-gap recovery keeps graph validation, deterministic repair, effect fail-closed handling, and budget enforcement in one auditable path"
    )]
    fn recover_style_control_gaps(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        mut loaded: LoadSessionResult,
    ) -> Result<LoadSessionResult, RunTurnError> {
        let Some(binding) = loaded.state.style_binding.as_ref() else {
            return Ok(loaded);
        };
        let executor =
            CompiledStyleExecutor::from_binding(binding).map_err(RunTurnError::StyleExecutor)?;
        let Some(adapter_kind) = executor.adapter_kind() else {
            // Unsupported graphs must remain mutation-free until their runtime
            // adapter exists.
            return Ok(loaded);
        };
        let max_steps = binding
            .budgets
            .max_steps
            .min(executor.compiled().graph.budget.max_steps);
        let Some(execution) = loaded.state.style_execution.as_ref() else {
            return Ok(loaded);
        };
        if execution.graph.as_ref() != &executor.compiled().graph {
            return Err(RunTurnError::StyleGraphMismatch);
        }
        let Some(execution) = loaded.state.style_execution.as_ref() else {
            return Ok(loaded);
        };
        if let StyleExecutionControlState::AwaitingTransition(completed) = &execution.control {
            let from = executor
                .node(&completed.node_id)
                .map_err(RunTurnError::StyleExecutor)?;
            let transition = executor
                .transition(from.index, &style_transition_variables(completed)?)
                .map_err(RunTurnError::StyleExecutor)?
                .ok_or_else(|| RunTurnError::UnexpectedStyleNode {
                    expected: "nonterminal graph transition",
                    actual: completed.node_id.clone(),
                })?;
            self.commit_next(
                persistence,
                session_id,
                session_directory,
                loaded.state.last_sequence,
                loaded.last_event_id,
                RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                    from_node_id: completed.node_id.clone(),
                    to_node_id: transition.to.id,
                    attempt: completed.attempt,
                    loop_iteration: completed.loop_iteration,
                    step: completed.step,
                }),
            )?;
            loaded = persistence
                .load_session(LoadSessionCommand {
                    session_directory: session_directory.to_owned(),
                    expected_session_id: session_id,
                })
                .map_err(RunTurnError::Persistence)?;
        }
        let Some(execution) = loaded.state.style_execution.as_ref() else {
            return Ok(loaded);
        };
        let StyleExecutionControlState::AwaitingDestinationEntry(selected) = &execution.control
        else {
            return Ok(loaded);
        };
        let destination = executor
            .node(&selected.to_node_id)
            .map_err(RunTurnError::StyleExecutor)?;
        let recoverable_fresh_model_entry = matches!(
            adapter_kind,
            StyleAdapterKind::EphemeralTurn | StyleAdapterKind::ResearchLoop
        ) && destination.directive
            == StyleNodeDirective::ModelCall
            && executor
                .node(&selected.from_node_id)
                .is_ok_and(|source| source.directive == StyleNodeDirective::ContextTransform);
        let recoverable_research_entry = matches!(
            adapter_kind,
            StyleAdapterKind::ResearchLoop
                | StyleAdapterKind::PlannerWorkerReviewer
                | StyleAdapterKind::DeclarativeGraph
        );
        if destination.directive.requires_effect_evidence()
            && !recoverable_fresh_model_entry
            && !recoverable_research_entry
        {
            return Err(RunTurnError::StyleControlRecoveryRequired {
                node: destination.id,
                phase: "awaiting_destination_entry",
            });
        }
        let step = selected
            .step
            .checked_add(1)
            .ok_or(RunTurnError::SequenceOverflow)?;
        let loop_iteration = if executor
            .node(&selected.from_node_id)
            .map_err(RunTurnError::StyleExecutor)?
            .directive
            == StyleNodeDirective::Loop
            && destination.directive != StyleNodeDirective::CompleteSession
        {
            selected
                .loop_iteration
                .checked_add(1)
                .ok_or(RunTurnError::SequenceOverflow)?
        } else {
            selected.loop_iteration
        };
        if step > max_steps {
            self.commit_style_budget_termination(
                persistence,
                session_id,
                session_directory,
                JournalPosition {
                    sequence: loaded.state.last_sequence,
                    event_id: loaded.last_event_id,
                },
                &destination.id,
                step,
                max_steps,
            )?;
            return Err(RunTurnError::StyleStepBudgetExceeded { limit: max_steps });
        }
        self.commit_next(
            persistence,
            session_id,
            session_directory,
            loaded.state.last_sequence,
            loaded.last_event_id,
            RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                node_id: destination.id,
                attempt: selected.attempt,
                loop_iteration,
                step,
            }),
        )?;
        persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)
    }

    fn resume_active_style_turn(
        state: &crate::session::SessionState,
        position: JournalPosition,
    ) -> Result<Option<ActiveStyleTurn>, RunTurnError> {
        let Some(binding) = state.style_binding.as_ref() else {
            return Ok(None);
        };
        let executor =
            CompiledStyleExecutor::from_binding(binding).map_err(RunTurnError::StyleExecutor)?;
        if executor.adapter_kind().is_none() {
            return Ok(None);
        }
        let canonical = state
            .style_execution
            .as_ref()
            .ok_or(RunTurnError::StyleGraphMismatch)?;
        let entered = canonical
            .active_node
            .as_ref()
            .ok_or_else(|| RunTurnError::StyleRecoveryRequired(String::from("<none>")))?;
        if canonical.graph.as_ref() != &executor.compiled().graph {
            return Err(RunTurnError::StyleGraphMismatch);
        }
        let current = executor
            .node(&entered.node_id)
            .map_err(RunTurnError::StyleExecutor)?;
        Ok(Some(ActiveStyleTurn {
            executor,
            current,
            position,
            attempt: entered.attempt,
            loop_iteration: entered.loop_iteration,
            step: entered.step,
            max_steps: binding
                .budgets
                .max_steps
                .min(canonical.graph.budget.max_steps),
        }))
    }

    fn complete_and_enter_next(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        execution: &mut ActiveStyleTurn,
        result_reference: Option<String>,
    ) -> Result<(), RunTurnError> {
        self.complete_and_enter_next_with(
            persistence,
            session_id,
            session_directory,
            execution,
            result_reference,
            None,
            &json!({}),
            false,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "compiled transition completion binds canonical counters, result identity, artifact identity, variables, and loop advancement"
    )]
    fn complete_and_enter_next_with(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        execution: &mut ActiveStyleTurn,
        result_reference: Option<String>,
        artifact_reference: Option<String>,
        variables: &Value,
        advance_loop: bool,
    ) -> Result<(), RunTurnError> {
        (execution.position.sequence, execution.position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            execution.position.sequence,
            execution.position.event_id,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: execution.current.id.clone(),
                attempt: execution.attempt,
                loop_iteration: execution.loop_iteration,
                step: execution.step,
                result_reference,
                artifact_reference,
            }),
        )?;
        let transition = execution
            .executor
            .transition(execution.current.index, variables)
            .map_err(RunTurnError::StyleExecutor)?
            .ok_or_else(|| RunTurnError::UnexpectedStyleNode {
                expected: "nonterminal graph transition",
                actual: execution.current.id.clone(),
            })?;
        (execution.position.sequence, execution.position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            execution.position.sequence,
            execution.position.event_id,
            RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                from_node_id: transition.from.id,
                to_node_id: transition.to.id.clone(),
                attempt: execution.attempt,
                loop_iteration: execution.loop_iteration,
                step: execution.step,
            }),
        )?;
        execution.step = execution
            .step
            .checked_add(1)
            .ok_or(RunTurnError::SequenceOverflow)?;
        execution.current = transition.to;
        if advance_loop {
            execution.loop_iteration = execution
                .loop_iteration
                .checked_add(1)
                .ok_or(RunTurnError::SequenceOverflow)?;
        }
        if execution.step > execution.max_steps {
            execution.position = self.commit_style_budget_termination(
                persistence,
                session_id,
                session_directory,
                execution.position,
                &execution.current.id,
                execution.step,
                execution.max_steps,
            )?;
            return Err(RunTurnError::StyleStepBudgetExceeded {
                limit: execution.max_steps,
            });
        }
        (execution.position.sequence, execution.position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            execution.position.sequence,
            execution.position.event_id,
            RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                node_id: execution.current.id.clone(),
                attempt: execution.attempt,
                loop_iteration: execution.loop_iteration,
                step: execution.step,
            }),
        )?;
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical termination explicitly binds journal position, refused node, and effective budget"
    )]
    fn commit_style_budget_termination(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        position: JournalPosition,
        refused_node_id: &str,
        refused_step: u64,
        limit: u64,
    ) -> Result<JournalPosition, RunTurnError> {
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            position.sequence,
            position.event_id,
            RuntimeCommittedEvent::StyleExecutionTerminated(StyleExecutionTerminatedEvent {
                reason: String::from("step_budget_exhausted"),
                refused_node_id: Some(refused_node_id.to_owned()),
                refused_step: Some(refused_step),
                limit: Some(limit),
            }),
        )?;
        Ok(JournalPosition { sequence, event_id })
    }

    fn complete_terminal_style_node(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        execution: &mut ActiveStyleTurn,
        result_reference: Option<String>,
    ) -> Result<(), RunTurnError> {
        if execution
            .executor
            .transition(execution.current.index, &json!({}))
            .map_err(RunTurnError::StyleExecutor)?
            .is_some()
        {
            return Err(RunTurnError::UnexpectedStyleNode {
                expected: "terminal graph node",
                actual: execution.current.id.clone(),
            });
        }
        (execution.position.sequence, execution.position.event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            execution.position.sequence,
            execution.position.event_id,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: execution.current.id.clone(),
                attempt: execution.attempt,
                loop_iteration: execution.loop_iteration,
                step: execution.step,
                result_reference,
                artifact_reference: None,
            }),
        )?;
        Ok(())
    }

    fn fail_style_node_at_head(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        execution: &ActiveStyleTurn,
        reason: &str,
        termination_reason: Option<&str>,
    ) -> Result<JournalPosition, RunTurnError> {
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        let active = loaded
            .state
            .style_execution
            .as_ref()
            .and_then(|state| state.active_node.as_ref())
            .filter(|active| active.node_id == execution.current.id)
            .ok_or_else(|| RunTurnError::StyleRecoveryRequired(execution.current.id.clone()))?;
        let (sequence, event_id) = self.commit_next(
            persistence,
            session_id,
            session_directory,
            loaded.state.last_sequence,
            loaded.last_event_id,
            RuntimeCommittedEvent::StyleNodeFailed(StyleNodeFailedEvent {
                node_id: active.node_id.clone(),
                attempt: active.attempt,
                loop_iteration: active.loop_iteration,
                step: active.step,
                reason: reason.to_owned(),
                artifact_reference: None,
                termination_reason: termination_reason.map(str::to_owned),
            }),
        )?;
        Ok(JournalPosition { sequence, event_id })
    }

    fn fail_active_bound_style_at_head(
        &self,
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        reason: &str,
    ) -> Result<Option<JournalPosition>, RunTurnError> {
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: session_directory.to_owned(),
                expected_session_id: session_id,
            })
            .map_err(RunTurnError::Persistence)?;
        let position = JournalPosition {
            sequence: loaded.state.last_sequence,
            event_id: loaded.last_event_id,
        };
        let Some(execution) = Self::resume_active_style_turn(&loaded.state, position)? else {
            return Ok(None);
        };
        self.fail_style_node_at_head(
            persistence,
            session_id,
            session_directory,
            &execution,
            reason,
            None,
        )
        .map(Some)
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
    ) -> Result<JournalPosition, RunTurnError> {
        let visible_text: String = events
            .iter()
            .filter_map(|event| match event {
                ProviderEvent::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect();
        if visible_text.is_empty() {
            return Ok(JournalPosition {
                sequence: previous_sequence,
                event_id: previous_event_id,
            });
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
        let event_id = event.metadata.event_id;
        persistence
            .commit_event(CommitSessionEventCommand {
                session_directory,
                event,
                durability: CommitDurability::Data,
            })
            .map_err(RunTurnError::Persistence)?;
        Ok(JournalPosition { sequence, event_id })
    }

    fn assistant_events_for_completion(
        persistence: &SessionPersistenceLogic<D>,
        session_id: SessionId,
        session_directory: &std::path::Path,
        style_turn: Option<&ActiveStyleTurn>,
        command: &RunTurnCommand,
        fallback: &[ProviderEvent],
    ) -> Result<Vec<ProviderEvent>, RunTurnError> {
        if !style_turn.is_some_and(|execution| {
            execution.executor.adapter_kind() == Some(StyleAdapterKind::EphemeralTurn)
        }) {
            return Ok(fallback.to_vec());
        }
        let state = Self::load_state(persistence, session_id, session_directory)?;
        let execution = state
            .style_execution
            .as_ref()
            .ok_or(RunTurnError::StyleGraphMismatch)?;
        let evidence = matching_ephemeral_model_evidence(&state, execution, command)
            .ok_or_else(|| RunTurnError::StyleRecoveryRequired(String::from("assistant")))?;
        Ok((!evidence.visible_text.is_empty())
            .then_some(ProviderEvent::Text(evidence.visible_text.clone()))
            .into_iter()
            .collect())
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
        Self::seal_event_with_identity(session_id, sequence, causation_id, identity, payload)
    }

    fn seal_event_with_identity(
        session_id: SessionId,
        sequence: Sequence,
        causation_id: Option<CausationId>,
        identity: EventIdentityDataRecord,
        payload: RuntimeCommittedEvent,
    ) -> Result<EventEnvelope<RuntimeCommittedEvent>, RunTurnError> {
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

fn recoverable_context_retry(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> bool {
    let Some(binding) = state.style_binding.as_ref() else {
        return false;
    };
    let Ok(executor) = CompiledStyleExecutor::from_binding(binding) else {
        return false;
    };
    let Some(adapter_kind) = executor.adapter_kind() else {
        return false;
    };
    let Some(execution) = state.style_execution.as_ref() else {
        return false;
    };
    let Some(active) = execution.active_node.as_ref() else {
        return false;
    };
    let Ok(request_hash) = current_context_request_hash(state, command) else {
        return false;
    };
    let boundary_at_head = execution.context_boundaries.iter().rev().any(|boundary| {
        boundary.identity.node_id == active.node_id
            && boundary.identity.run_id == command.cancellation_id
            && boundary.identity.origin == expected_initial_context_origin(state)
            && request_hash == boundary.identity.request_hash
            && boundary.last_sequence == state.last_sequence
    });
    let pristine_entry = execution.active_node_entered_at == Some(state.last_sequence);
    let active_kind = execution
        .graph
        .nodes
        .iter()
        .find(|node| node.id == active.node_id)
        .map(|node| node.kind);
    match (adapter_kind, active_kind) {
        (
            StyleAdapterKind::PersistentTurn | StyleAdapterKind::PlannerWorkerReviewer,
            Some(
                agentmod_graph_engine::NodeKind::ModelCall
                | agentmod_graph_engine::NodeKind::Review,
            ),
        )
        | (
            StyleAdapterKind::EphemeralTurn | StyleAdapterKind::ResearchLoop,
            Some(agentmod_graph_engine::NodeKind::ContextTransform),
        ) => pristine_entry || boundary_at_head,
        (
            StyleAdapterKind::EphemeralTurn | StyleAdapterKind::ResearchLoop,
            Some(agentmod_graph_engine::NodeKind::ModelCall),
        ) => {
            boundary_at_head
                || (pristine_entry
                    && execution.context_boundaries.iter().rev().any(|boundary| {
                        boundary.completed_at.is_some()
                            && boundary.identity.boundary == "turn_start"
                            && boundary.identity.run_id == command.cancellation_id
                            && boundary.identity.origin == expected_initial_context_origin(state)
                            && boundary.identity.request_hash == request_hash
                            && exact_compiled_edge(
                                &execution.graph,
                                &boundary.identity.node_id,
                                &active.node_id,
                                agentmod_graph_engine::NodeKind::ContextTransform,
                                agentmod_graph_engine::NodeKind::ModelCall,
                            )
                    }))
        }
        _ => false,
    }
}

fn exact_compiled_edge(
    graph: &agentmod_graph_engine::ExecutableGraph,
    from_id: &str,
    to_id: &str,
    from_kind: agentmod_graph_engine::NodeKind,
    to_kind: agentmod_graph_engine::NodeKind,
) -> bool {
    let Some(from) = graph
        .nodes
        .iter()
        .find(|node| node.id == from_id && node.kind == from_kind)
    else {
        return false;
    };
    let Some(to) = graph
        .nodes
        .iter()
        .find(|node| node.id == to_id && node.kind == to_kind)
    else {
        return false;
    };
    graph
        .edges
        .iter()
        .any(|edge| edge.from == from.index && edge.to == to.index)
}

fn recoverable_ephemeral_discard(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> bool {
    let Some(execution) = state.style_execution.as_ref() else {
        return false;
    };
    let Some(active) = execution.active_node.as_ref() else {
        return false;
    };
    let Ok(request_hash) = current_context_request_hash(state, command) else {
        return false;
    };
    let is_complete_turn = execution.graph.nodes.iter().any(|node| {
        node.id == active.node_id && node.kind == agentmod_graph_engine::NodeKind::CompleteTurn
    });
    let Some(provenance) = state.conversation.projection_provenance() else {
        return false;
    };
    let Some(run_id) = provenance
        .projection_id
        .strip_prefix("ephemeral-turn-discard:")
        .and_then(|suffix| suffix.rsplit_once(':').map(|(run_id, _)| run_id))
    else {
        return false;
    };
    let Some(model_evidence) = matching_ephemeral_model_evidence(state, execution, command) else {
        return false;
    };
    let Some(completed_at) = model_evidence.completed_at else {
        return false;
    };
    let latest_assistant_matches = state.conversation.history().iter().rev().any(|entry| {
        matches!(
            entry,
            ConversationEntry::AssistantMessage(assistant)
                if assistant.source_sequence > completed_at
                    &&
                assistant.id.0.strip_prefix("assistant:").is_some_and(
                    |suffix| suffix.ends_with(&format!(":{run_id}"))
                )
        )
    });
    let completed_boundary_at_head = execution.context_boundaries.last().is_some_and(|boundary| {
        boundary.identity.node_id == active.node_id
            && boundary.identity.boundary == "before_turn_completion"
            && boundary.identity.origin == expected_initial_context_origin(state)
            && boundary.identity.run_id == run_id
            && boundary.identity.run_id == command.cancellation_id
            && boundary.identity.request_hash == request_hash
            && boundary.completed_phases.as_slice() == ["discard"]
            && boundary.completed_at == Some(state.last_sequence)
    });
    is_complete_turn
        && provenance.method == "ephemeral_discard"
        && provenance.committed_at.checked_next().ok() == Some(state.last_sequence)
        && state.conversation.provider_projection().is_empty()
        && (latest_assistant_matches || model_evidence.visible_text.is_empty())
        && completed_boundary_at_head
}

fn recoverable_ephemeral_discard_phase(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> bool {
    let Some(execution) = state.style_execution.as_ref() else {
        return false;
    };
    let Some(active) = execution.active_node.as_ref() else {
        return false;
    };
    let is_complete_turn = execution.graph.nodes.iter().any(|node| {
        node.id == active.node_id && node.kind == agentmod_graph_engine::NodeKind::CompleteTurn
    });
    let Some(boundary) = execution.context_boundaries.last() else {
        return false;
    };
    let Some(provenance) = state.conversation.projection_provenance() else {
        return false;
    };
    let Ok(request_hash) = current_context_request_hash(state, command) else {
        return false;
    };
    let Some(model_evidence) = matching_ephemeral_model_evidence(state, execution, command) else {
        return false;
    };
    let Some(completed_at) = model_evidence.completed_at else {
        return false;
    };
    let assistant_matches = state.conversation.history().iter().rev().any(|entry| {
        matches!(
            entry,
            ConversationEntry::AssistantMessage(assistant)
                if assistant.source_sequence > completed_at
                    && assistant.id.0.strip_prefix("assistant:").is_some_and(
                        |suffix| suffix.ends_with(&format!(":{}", command.cancellation_id))
                    )
        )
    });
    is_complete_turn
        && boundary.identity.node_id == active.node_id
        && boundary.identity.boundary == "before_turn_completion"
        && boundary.identity.origin == expected_initial_context_origin(state)
        && boundary.identity.run_id == command.cancellation_id
        && boundary.identity.request_hash == request_hash
        && boundary.started_phases.as_slice() == ["discard"]
        && boundary.completed_phases.as_slice() == ["discard"]
        && boundary.completed_at.is_none()
        && boundary.last_sequence == state.last_sequence
        && provenance.method == "ephemeral_discard"
        && provenance.committed_at == state.last_sequence
        && provenance.projection_id.starts_with(&format!(
            "ephemeral-turn-discard:{}:",
            boundary.identity.run_id
        ))
        && state.conversation.provider_projection().is_empty()
        && (assistant_matches || model_evidence.visible_text.is_empty())
}

fn matching_ephemeral_model_evidence<'a>(
    state: &crate::session::SessionState,
    execution: &'a crate::session::StyleExecutionState,
    command: &RunTurnCommand,
) -> Option<&'a crate::session::ModelExecutionEvidence> {
    let input_sequence = current_run_input_sequence(state, command).ok()?;
    let request_hash = current_context_request_hash(state, command).ok()?;
    let evidence = execution.latest_model_execution.as_ref()?;
    let completed_at = evidence.completed_at?;
    (evidence.cancellation_id == command.cancellation_id
        && evidence.response_completed
        && evidence.user_sequence == Some(input_sequence)
        && evidence.started_at > input_sequence
        && completed_at >= evidence.started_at
        && completed_at < state.last_sequence)
        .then_some(())?;
    execution.context_boundaries.iter().rev().find(|boundary| {
        boundary.identity.boundary == "before_model_request"
            && boundary.identity.origin == expected_initial_context_origin(state)
            && boundary.identity.run_id == command.cancellation_id
            && boundary.identity.request_hash == request_hash
            && boundary
                .completed_at
                .is_some_and(|sequence| sequence > input_sequence && sequence < evidence.started_at)
            && execution.graph.nodes.iter().any(|node| {
                node.id == boundary.identity.node_id
                    && node.kind == agentmod_graph_engine::NodeKind::ModelCall
            })
    })?;
    Some(evidence)
}

fn recoverable_ephemeral_pre_assistant(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Option<String> {
    let binding = state.style_binding.as_ref()?;
    let executor = CompiledStyleExecutor::from_binding(binding).ok()?;
    (executor.adapter_kind() == Some(StyleAdapterKind::EphemeralTurn)).then_some(())?;
    let execution = state.style_execution.as_ref()?;
    let active = execution.active_node.as_ref()?;
    (execution.active_node_entered_at == Some(state.last_sequence)).then_some(())?;
    execution
        .graph
        .nodes
        .iter()
        .any(|node| {
            node.id == active.node_id && node.kind == agentmod_graph_engine::NodeKind::CompleteTurn
        })
        .then_some(())?;
    let evidence = matching_ephemeral_model_evidence(state, execution, command)?;
    let completed_at = evidence.completed_at?;
    let completed_tool = execution.completed_nodes.last()?;
    let transition = execution.transitions.last()?;
    (execution.graph.nodes.iter().any(|node| {
        node.id == completed_tool.node_id
            && node.kind == agentmod_graph_engine::NodeKind::ToolExecutionGate
    }) && transition.from_node_id == completed_tool.node_id
        && transition.to_node_id == active.node_id
        && completed_tool.step.checked_add(1) == Some(active.step)
        && transition.step == completed_tool.step
        && exact_compiled_edge(
            &execution.graph,
            &completed_tool.node_id,
            &active.node_id,
            agentmod_graph_engine::NodeKind::ToolExecutionGate,
            agentmod_graph_engine::NodeKind::CompleteTurn,
        ))
    .then_some(())?;
    state
        .tool_executions
        .values()
        .all(|record| record.state == ToolExecutionState::Terminal)
        .then_some(())?;
    (!state.conversation.history().iter().any(|entry| {
        matches!(
            entry,
            ConversationEntry::AssistantMessage(assistant)
                if assistant.source_sequence > completed_at
                    && assistant.id.0.strip_prefix("assistant:").is_some_and(
                        |suffix| suffix.ends_with(&format!(":{}", command.cancellation_id))
                    )
        )
    }))
    .then(|| evidence.visible_text.clone())
}

fn recoverable_ephemeral_cleanup_retry(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> bool {
    let Some(binding) = state.style_binding.as_ref() else {
        return false;
    };
    let Ok(executor) = CompiledStyleExecutor::from_binding(binding) else {
        return false;
    };
    if executor.adapter_kind() != Some(StyleAdapterKind::EphemeralTurn) {
        return false;
    }
    let Some(execution) = state.style_execution.as_ref() else {
        return false;
    };
    let Some(active) = execution.active_node.as_ref() else {
        return false;
    };
    if !execution.graph.nodes.iter().any(|node| {
        node.id == active.node_id && node.kind == agentmod_graph_engine::NodeKind::CompleteTurn
    }) {
        return false;
    }
    let Ok(request_hash) = current_context_request_hash(state, command) else {
        return false;
    };
    let Some(model_evidence) = matching_ephemeral_model_evidence(state, execution, command) else {
        return false;
    };
    let Some(model_completed_at) = model_evidence.completed_at else {
        return false;
    };
    let latest_assistant_is_head = state.conversation.history().iter().rev().any(|entry| {
        matches!(
            entry,
            ConversationEntry::AssistantMessage(assistant)
                if assistant.source_sequence == state.last_sequence
                    && assistant.source_sequence > model_completed_at
                    && assistant.id.0.strip_prefix("assistant:").is_some_and(
                        |suffix| suffix.ends_with(&format!(":{}", command.cancellation_id))
                    )
        )
    });
    let pristine_discard_boundary = execution.context_boundaries.last().is_some_and(|boundary| {
        boundary.identity.node_id == active.node_id
            && boundary.identity.boundary == "before_turn_completion"
            && boundary.identity.run_id == command.cancellation_id
            && boundary.identity.origin == expected_initial_context_origin(state)
            && boundary.identity.request_hash == request_hash
            && boundary.started_phases.is_empty()
            && boundary.completed_phases.is_empty()
            && boundary.completed_at.is_none()
            && boundary.last_sequence == state.last_sequence
    });
    latest_assistant_is_head || pristine_discard_boundary
}

fn latest_context_boundary_at_head(
    state: &crate::session::SessionState,
) -> Option<&ContextBoundaryIdentity> {
    state
        .style_execution
        .as_ref()?
        .context_boundaries
        .iter()
        .rev()
        .find(|boundary| boundary.last_sequence == state.last_sequence)
        .map(|boundary| &boundary.identity)
}

fn context_phase_started(
    state: &crate::session::SessionState,
    identity: &ContextBoundaryIdentity,
    phase: &str,
) -> bool {
    state
        .style_execution
        .as_ref()
        .and_then(|execution| {
            execution
                .context_boundaries
                .iter()
                .rev()
                .find(|boundary| &boundary.identity == identity)
        })
        .is_some_and(|boundary| {
            boundary
                .started_phases
                .iter()
                .any(|started| started == phase)
                && !boundary
                    .completed_phases
                    .iter()
                    .any(|completed| completed == phase)
        })
}

fn current_run_user_sequence(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<Sequence, RunTurnError> {
    current_run_input_sequence(state, command)
}

fn current_run_input_sequence(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<Sequence, RunTurnError> {
    current_run_user(state, command)
        .map(|user| user.source_sequence)
        .or_else(|_| {
            state
                .child_origin
                .as_ref()
                .filter(|origin| origin.task == command.prompt)
                .map(|origin| origin.linked_at)
                .ok_or(RunTurnError::ContextRecoveryIdentityMismatch)
        })
}

fn current_provider_input_entry(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<ConversationEntry, RunTurnError> {
    if let Ok(user) = current_run_user(state, command) {
        return Ok(ConversationEntry::UserMessage(user.clone()));
    }
    let origin = state
        .child_origin
        .as_ref()
        .filter(|origin| origin.task == command.prompt)
        .ok_or(RunTurnError::ContextRecoveryIdentityMismatch)?;
    Ok(ConversationEntry::PendingTask(PendingTaskEntry {
        id: ConversationEntryId(format!("child-task:{}:{}", origin.task_id, origin.revision)),
        task_id: origin.task_id.clone(),
        description: origin.task.clone(),
        state: String::from("assigned"),
        source_sequence: origin.linked_at,
    }))
}

fn validate_child_task_input(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<(), RunTurnError> {
    let origin = state
        .child_origin
        .as_ref()
        .ok_or(RunTurnError::ChildTaskRequired)?;
    if origin.task != command.prompt
        || origin.input_hash != ContentHash::digest(command.prompt.as_bytes())
        || state.lifecycle != crate::session::SessionLifecycle::Active
    {
        return Err(RunTurnError::ChildTaskMismatch);
    }
    Ok(())
}

fn current_run_user<'a>(
    state: &'a crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<&'a TextEntry, RunTurnError> {
    let exact = state
        .conversation
        .history()
        .iter()
        .rev()
        .find_map(|entry| match entry {
            ConversationEntry::UserMessage(user)
                if (command.prompt.is_empty() || user.text == command.prompt)
                    && user.id.0.strip_prefix("user:").is_some_and(|suffix| {
                        suffix.ends_with(&format!(":{}", command.cancellation_id))
                    }) =>
            {
                Some(user)
            }
            _ => None,
        });
    if exact.is_some() {
        return exact.ok_or(RunTurnError::ContextRecoveryIdentityMismatch);
    }
    let base_run_id = research_base_run_id(&command.cancellation_id)
        .or_else(|| planner_base_run_id(&command.cancellation_id))
        .or_else(|| research_base_run_id_from_state(state))
        .or_else(|| planner_base_run_id_from_state(state))
        .ok_or(RunTurnError::ContextRecoveryIdentityMismatch)?;
    state
        .conversation
        .history()
        .iter()
        .rev()
        .find_map(|entry| match entry {
            ConversationEntry::UserMessage(user)
                if (command.prompt.is_empty() || user.text == command.prompt)
                    && user
                        .id
                        .0
                        .strip_prefix("user:")
                        .is_some_and(|suffix| suffix.ends_with(&format!(":{base_run_id}"))) =>
            {
                Some(user)
            }
            _ => None,
        })
        .ok_or(RunTurnError::ContextRecoveryIdentityMismatch)
}

fn current_context_request_hash(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<ContentHash, RunTurnError> {
    let source_sequence = current_run_input_sequence(state, command)?;
    let current_input = current_run_user(state, command)
        .map(|user| user.text.as_str())
        .or_else(|_| {
            state
                .child_origin
                .as_ref()
                .filter(|origin| origin.task == command.prompt)
                .map(|origin| origin.task.as_str())
                .ok_or(RunTurnError::ContextRecoveryIdentityMismatch)
        })?;
    context_request_hash(command, source_sequence, current_input)
}

fn context_request_hash(
    command: &RunTurnCommand,
    user_sequence: Sequence,
    current_input: &str,
) -> Result<ContentHash, RunTurnError> {
    let canonical_options =
        canonical_json_bytes(&command.options).map_err(map_projection_measure_error)?;
    let input_hash = ContentHash::digest(current_input.as_bytes());
    let mut identity = Vec::from(b"agentmod.context-request.v1".as_slice());
    append_identity_field(&mut identity, command.provider.as_bytes())?;
    append_identity_field(&mut identity, command.model.as_bytes())?;
    append_identity_field(&mut identity, &canonical_options)?;
    identity.extend_from_slice(&user_sequence.get().to_le_bytes());
    identity.extend_from_slice(input_hash.as_bytes());
    Ok(ContentHash::digest(&identity))
}

fn append_identity_field(target: &mut Vec<u8>, value: &[u8]) -> Result<(), RunTurnError> {
    let length = u64::try_from(value.len()).map_err(|_| RunTurnError::ProjectionSizeOverflow)?;
    target.extend_from_slice(&length.to_le_bytes());
    target.extend_from_slice(value);
    Ok(())
}

const fn boundary_origin(origin: ContextCompositionOrigin) -> ContextBoundaryOrigin {
    match origin {
        ContextCompositionOrigin::UserTurn => ContextBoundaryOrigin::UserTurn,
        ContextCompositionOrigin::ChildTask => ContextBoundaryOrigin::ChildTask,
        ContextCompositionOrigin::ToolContinuation => ContextBoundaryOrigin::ToolContinuation,
        ContextCompositionOrigin::ApprovalContinuation => {
            ContextBoundaryOrigin::ApprovalContinuation
        }
    }
}

fn expected_initial_context_origin(state: &crate::session::SessionState) -> ContextBoundaryOrigin {
    if state.child_origin.is_some() {
        ContextBoundaryOrigin::ChildTask
    } else {
        ContextBoundaryOrigin::UserTurn
    }
}

fn pending_model_resume_after_terminal_tool(
    state: &crate::session::SessionState,
    tool: Option<&crate::session::ToolExecutionRecord>,
) -> Result<bool, RunTurnError> {
    let Some(terminal_at) = tool.and_then(|record| record.terminal_at) else {
        return Ok(false);
    };
    let Some(execution) = state.style_execution.as_ref() else {
        return Ok(false);
    };
    let Some(active) = execution.active_node.as_ref() else {
        return Ok(false);
    };
    if !execution.graph.nodes.iter().any(|node| {
        node.id == active.node_id && node.kind == agentmod_graph_engine::NodeKind::ToolExecutionGate
    }) {
        return Ok(false);
    }
    if let Some(boundary) = execution.context_boundaries.iter().rev().find(|boundary| {
        boundary.identity.node_id == active.node_id
            && boundary.identity.boundary == "before_model_request"
            && boundary.identity.source_head >= terminal_at
    }) {
        if boundary.completed_at.is_some() && boundary.last_sequence < state.last_sequence {
            return Err(RunTurnError::AmbiguousProviderResume);
        }
        if boundary.completed_at.is_none() {
            return Err(RunTurnError::AmbiguousContextPhase(String::from(
                "approval_resume",
            )));
        }
    }
    Ok(true)
}

fn terminal_projection(
    terminal: &crate::session::ToolExecutionRecord,
    call_id: &str,
) -> Result<(Value, Option<String>, bool), RunTurnError> {
    match terminal.terminal_outcome.as_ref() {
        Some(ToolExecutionTerminalOutcome::Completed {
            result,
            artifact,
            truncated,
        }) => Ok((result.clone(), artifact.clone(), *truncated)),
        Some(ToolExecutionTerminalOutcome::Failed {
            code,
            message,
            retryable,
        }) => Ok((
            json!({"error":{"code":code,"message":message,"retryable":retryable}}),
            None,
            false,
        )),
        None => Err(RunTurnError::ToolConversationRecoveryConflict(
            call_id.to_owned(),
        )),
    }
}

fn project_tool_result(
    result: &Value,
    artifact: Option<&str>,
    truncated: bool,
) -> Result<(String, Option<agentmod_primitives::ArtifactId>, bool), RunTurnError> {
    let mut content =
        serde_json::to_string(result).map_err(|_| RunTurnError::ToolResultEncoding)?;
    let projection_truncated = content.len() > MAX_TOOL_RESULT_BYTES;
    if projection_truncated {
        truncate_owned_utf8(&mut content, MAX_TOOL_RESULT_BYTES);
    }
    let artifact_id = artifact
        .map(str::parse)
        .transpose()
        .map_err(|_| RunTurnError::InvalidArtifact)?;
    Ok((content, artifact_id, truncated || projection_truncated))
}

fn memory_scope(
    scope: &str,
    session_id: SessionId,
    workspace: &str,
) -> Result<MemoryScope, RunTurnError> {
    match scope {
        "session" => Ok(MemoryScope::Session(session_id.to_string())),
        "project" => Ok(MemoryScope::Project(workspace.to_owned())),
        "runtime" => Ok(MemoryScope::Runtime),
        "user" => Err(RunTurnError::MemoryScopeIdentityUnavailable(String::from(
            "user",
        ))),
        other => Err(RunTurnError::UnsupportedMemoryScope(other.to_owned())),
    }
}

fn construct_memory_query(
    binding: &crate::session::SessionStyleBinding,
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<String, RunTurnError> {
    let configuration: Value = serde_json::from_str(&binding.memory.query_json)
        .map_err(|_| RunTurnError::InvalidMemoryQueryConfiguration)?;
    let source = configuration
        .get("source")
        .and_then(Value::as_str)
        .ok_or(RunTurnError::InvalidMemoryQueryConfiguration)?;
    let mut query = match source {
        "current_input" => {
            if command.prompt.is_empty() {
                state
                    .conversation
                    .history()
                    .iter()
                    .rev()
                    .find_map(|entry| match entry {
                        ConversationEntry::UserMessage(user) => Some(user.text.clone()),
                        _ => None,
                    })
                    .ok_or(RunTurnError::CurrentInputMissing)?
            } else {
                command.prompt.clone()
            }
        }
        "session_goal" => state
            .conversation
            .history()
            .iter()
            .find_map(|entry| match entry {
                ConversationEntry::UserMessage(user) => Some(user.text.clone()),
                _ => None,
            })
            .ok_or(RunTurnError::MemorySessionGoalUnavailable)?,
        "current_input_and_goal" => {
            let goal = state
                .conversation
                .history()
                .iter()
                .find_map(|entry| match entry {
                    ConversationEntry::UserMessage(user) => Some(user.text.as_str()),
                    _ => None,
                })
                .ok_or(RunTurnError::MemorySessionGoalUnavailable)?;
            format!("current input: {}\nsession goal: {goal}", command.prompt)
        }
        "explicit" => return Err(RunTurnError::ExplicitMemoryQueryRequired),
        _ => return Err(RunTurnError::InvalidMemoryQueryConfiguration),
    };
    if configuration
        .get("include_active_artifacts")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let labels = state
            .conversation
            .provider_projection()
            .iter()
            .filter_map(|entry| match entry {
                ConversationEntry::Attachment(value)
                | ConversationEntry::Image(value)
                | ConversationEntry::ArtifactReference(value) => Some(value.label.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if !labels.is_empty() {
            query.push_str("\nactive artifacts: ");
            query.push_str(&labels.join(", "));
        }
    }
    if configuration
        .get("include_style_context")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        query.push_str("\nstyle: ");
        query.push_str(&binding.id);
        if let Some(active) = state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.active_node.as_ref())
        {
            query.push_str("\nnode: ");
            query.push_str(&active.node_id);
        }
    }
    let max_bytes = configuration
        .get("max_query_bytes")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(RunTurnError::InvalidMemoryQueryConfiguration)?;
    truncate_owned_utf8(&mut query, max_bytes.min(MAX_MEMORY_QUERY_BYTES));
    if query.trim().is_empty() {
        return Err(RunTurnError::InvalidMemoryQueryConfiguration);
    }
    Ok(query)
}

fn inject_memory(
    replacement: &mut Vec<ConversationEntry>,
    entries: Vec<ConversationEntry>,
    location: &str,
) -> Result<(), RunTurnError> {
    match location {
        "before_conversation" => replacement.splice(0..0, entries),
        "after_conversation" => replacement.splice(replacement.len().., entries),
        "before_current_input" => {
            let index = replacement
                .iter()
                .rposition(|entry| matches!(entry, ConversationEntry::UserMessage(_)))
                .ok_or(RunTurnError::CurrentInputMissing)?;
            replacement.splice(index..index, entries)
        }
        "none" => return Ok(()),
        other => return Err(RunTurnError::UnsupportedMemoryInjection(other.to_owned())),
    };
    Ok(())
}

fn serialized_entry_contribution(entry: &ConversationEntry) -> Result<u64, RunTurnError> {
    u64::try_from(
        serde_json::to_vec(entry)
            .map_err(|_| RunTurnError::Event)?
            .len(),
    )
    .map_err(|_| RunTurnError::MemoryBoundOverflow)
}

fn memory_entry_with_serialized_size(
    mut entry: ConversationEntry,
) -> Result<(ConversationEntry, u64), RunTurnError> {
    loop {
        let contribution = serialized_entry_contribution(&entry)?;
        let ConversationEntry::RetrievedMemory(memory) = &mut entry else {
            return Err(RunTurnError::Event);
        };
        if memory.size_bytes == contribution {
            return Ok((entry, contribution));
        }
        memory.size_bytes = contribution;
    }
}

fn projection_without_retrieved_memory(entries: &[ConversationEntry]) -> Vec<ConversationEntry> {
    entries
        .iter()
        .filter(|entry| !matches!(entry, ConversationEntry::RetrievedMemory(_)))
        .cloned()
        .collect()
}

fn validate_projection_measure(
    entries: &[ConversationEntry],
    token_limit: Option<u64>,
) -> Result<ProjectionMeasure, RunTurnError> {
    let measure = measure_projection(entries).map_err(map_projection_measure_error)?;
    if measure.serialized_bytes > MAX_PROVIDER_PROJECTION_BYTES {
        return Err(RunTurnError::ProviderProjectionByteLimitExceeded {
            serialized_bytes: measure.serialized_bytes,
            limit: MAX_PROVIDER_PROJECTION_BYTES,
        });
    }
    if token_limit.is_some_and(|limit| measure.estimated_tokens > limit) {
        return Err(RunTurnError::ProviderProjectionLimitExceeded {
            estimated_tokens: measure.estimated_tokens,
            limit: token_limit.unwrap_or(0),
        });
    }
    Ok(measure)
}

const fn map_projection_measure_error(error: ProjectionMeasureError) -> RunTurnError {
    match error {
        ProjectionMeasureError::Serialization => RunTurnError::Event,
        ProjectionMeasureError::Overflow => RunTurnError::ProjectionSizeOverflow,
    }
}

fn effective_projection_limit(
    compaction: &crate::session::SessionCompactionConfiguration,
) -> Result<Option<u64>, RunTurnError> {
    if compaction.max_provider_projection_tokens == 0 {
        if compaction.reserved_context_tokens == 0 {
            return Ok(None);
        }
        return Err(RunTurnError::InvalidProjectionBudget);
    }
    compaction
        .max_provider_projection_tokens
        .checked_sub(compaction.reserved_context_tokens)
        .filter(|limit| *limit > 0)
        .map(Some)
        .ok_or(RunTurnError::InvalidProjectionBudget)
}

fn compact_sliding_window_to_bound(
    conversation: &crate::conversation::ConversationState,
    requirements: &[String],
    limit: Option<u64>,
    context: &CompactionContext,
) -> Result<crate::compaction::CompactionPlan, RunTurnError> {
    let source = conversation.provider_projection();
    let maximum_window = DEFAULT_SLIDING_WINDOW_ENTRIES.min(source.len().max(1));
    for window in (1..=maximum_window).rev() {
        let mut plan = compact_projection(
            conversation,
            CompactionStrategy::SlidingWindow {
                max_recent_entries: window,
            },
            context.clone(),
        )
        .map_err(RunTurnError::Compaction)?;
        plan.replacement =
            restore_required_projection_entries(source, &plan.replacement, requirements);
        validate_projection_preservation(source, &plan.replacement, requirements)?;
        let measure =
            measure_projection(&plan.replacement).map_err(map_projection_measure_error)?;
        if measure.serialized_bytes <= MAX_PROVIDER_PROJECTION_BYTES
            && limit.is_none_or(|bound| measure.estimated_tokens <= bound)
        {
            return Ok(plan);
        }
    }
    let minimum = restore_required_projection_entries(source, &[], requirements);
    let measure = measure_projection(&minimum).map_err(map_projection_measure_error)?;
    if measure.serialized_bytes > MAX_PROVIDER_PROJECTION_BYTES {
        return Err(RunTurnError::ProviderProjectionByteLimitExceeded {
            serialized_bytes: measure.serialized_bytes,
            limit: MAX_PROVIDER_PROJECTION_BYTES,
        });
    }
    Err(RunTurnError::ProviderProjectionLimitExceeded {
        estimated_tokens: measure.estimated_tokens,
        limit: limit.unwrap_or(0),
    })
}

fn restore_required_projection_entries(
    source: &[ConversationEntry],
    candidate: &[ConversationEntry],
    requirements: &[String],
) -> Vec<ConversationEntry> {
    let selected = candidate
        .iter()
        .map(|entry| entry.id().clone())
        .collect::<BTreeSet<_>>();
    let current_input = source
        .iter()
        .rfind(|entry| matches!(entry, ConversationEntry::UserMessage(_)))
        .map(ConversationEntry::id);
    source
        .iter()
        .filter(|entry| {
            selected.contains(entry.id())
                || projection_entry_is_required(entry, current_input, requirements)
        })
        .cloned()
        .collect()
}

fn validate_projection_preservation(
    source: &[ConversationEntry],
    replacement: &[ConversationEntry],
    requirements: &[String],
) -> Result<(), RunTurnError> {
    let retained = replacement
        .iter()
        .map(ConversationEntry::id)
        .collect::<BTreeSet<_>>();
    let current_input = source
        .iter()
        .rfind(|entry| matches!(entry, ConversationEntry::UserMessage(_)))
        .map(ConversationEntry::id);
    if let Some(missing) = source.iter().find(|entry| {
        projection_entry_is_required(entry, current_input, requirements)
            && !retained.contains(entry.id())
    }) {
        return Err(RunTurnError::CompactionPreservationViolation(
            missing.id().0.clone(),
        ));
    }
    Ok(())
}

fn projection_entry_is_required(
    entry: &ConversationEntry,
    current_input: Option<&ConversationEntryId>,
    requirements: &[String],
) -> bool {
    requirements
        .iter()
        .any(|requirement| match requirement.as_str() {
            "system_instructions" => matches!(
                entry,
                ConversationEntry::SystemInstruction(_)
                    | ConversationEntry::ProjectInstruction(_)
                    | ConversationEntry::UserInstruction(_)
            ),
            "current_input" => current_input.is_some_and(|id| id == entry.id()),
            "pending_control_state" => matches!(
                entry,
                ConversationEntry::PendingTask(_)
                    | ConversationEntry::ActiveProcessSummary(_)
                    | ConversationEntry::ChildAgentHandoff(_)
            ),
            "artifact_references" => matches!(
                entry,
                ConversationEntry::Attachment(_)
                    | ConversationEntry::Image(_)
                    | ConversationEntry::ArtifactReference(_)
            ),
            "memory_provenance" => matches!(entry, ConversationEntry::RetrievedMemory(_)),
            // Active graph state is canonical style control state rather than a
            // provider-projection entry, so there is nothing representable here
            // to drop or preserve.
            "active_graph_state" => active_graph_state_is_outside_projection(),
            "tool_call_correlation" => matches!(
                entry,
                ConversationEntry::ToolCallRequest(_) | ConversationEntry::ToolResult(_)
            ),
            _ => false,
        })
}

fn truncate_owned_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
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
        (
            ApprovalDisposition::Denied | ApprovalDisposition::Approved,
            ApprovalState::Pending,
            _,
        ) => ApprovalRecoveryAction::CommitAndResume,
        (ApprovalDisposition::Denied, ApprovalState::Denied, None)
        | (ApprovalDisposition::Approved, ApprovalState::Approved, None) => {
            ApprovalRecoveryAction::Resume
        }
        (
            ApprovalDisposition::Denied,
            ApprovalState::Denied,
            Some(ToolExecutionState::Terminal),
        )
        | (
            ApprovalDisposition::Approved,
            ApprovalState::Approved,
            Some(ToolExecutionState::Terminal),
        ) => ApprovalRecoveryAction::Idempotent,
        (
            ApprovalDisposition::Denied,
            ApprovalState::Denied,
            Some(ToolExecutionState::Dispatched | ToolExecutionState::Started),
        )
        | (ApprovalDisposition::Denied, ApprovalState::Approved, _)
        | (ApprovalDisposition::Approved, ApprovalState::Denied, _) => {
            ApprovalRecoveryAction::Invalid
        }
        (
            ApprovalDisposition::Approved,
            ApprovalState::Approved,
            Some(ToolExecutionState::Dispatched | ToolExecutionState::Started),
        ) => ApprovalRecoveryAction::Reconcile,
    }
}

fn invalid_replacement() -> RunTurnError {
    RunTurnError::Provider(ProviderExecutionError::InvalidInterceptionReplacement)
}

fn provider_node_failure(events: &[ProviderEvent]) -> Option<&'static str> {
    events.iter().find_map(|event| match event {
        ProviderEvent::Cancelled => Some("model_request_cancelled"),
        ProviderEvent::Failed { .. } => Some("model_request_failed"),
        ProviderEvent::Started
        | ProviderEvent::Text(_)
        | ProviderEvent::ToolDelta { .. }
        | ProviderEvent::ToolProposed { .. }
        | ProviderEvent::Completed { .. } => None,
    })
}

fn validate_planner_child_policy(
    compiled: &agentmod_session_style_sdk::CompiledSessionStyle,
) -> Result<(), RunTurnError> {
    let children = &compiled.child_agents;
    if children.max_children < 2
        || children.max_concurrent == 0
        || children.max_depth == 0
        || children.per_child_token_budget == 0
        || children.child_style.is_none()
        || children.workspace_mode
            != Some(agentmod_session_style_sdk::ChildWorkspaceMode::SharedReadOnly)
        || children.inherit_provider != Some(true)
        || children.inherit_model != Some(true)
        || children.context_budget_tokens.is_none()
        || children.memory_access.is_none()
        || children.join_behavior != Some(agentmod_session_style_sdk::ChildJoinBehavior::All)
        || children.cancellation_behavior
            != Some(agentmod_session_style_sdk::ChildCancellationBehavior::Cascade)
        || children.reviewer_max_attempts.is_none()
    {
        return Err(RunTurnError::InvalidChildPolicy);
    }
    Ok(())
}

fn child_workspace_mode(mode: agentmod_session_style_sdk::ChildWorkspaceMode) -> String {
    match mode {
        agentmod_session_style_sdk::ChildWorkspaceMode::SharedReadOnly => {
            String::from("shared_read_only")
        }
        agentmod_session_style_sdk::ChildWorkspaceMode::SharedSerializedWrites => {
            String::from("shared_serialized_writes")
        }
        agentmod_session_style_sdk::ChildWorkspaceMode::IndependentGitWorktree => {
            String::from("independent_git_worktree")
        }
        agentmod_session_style_sdk::ChildWorkspaceMode::TemporaryCopy => {
            String::from("temporary_copy")
        }
        agentmod_session_style_sdk::ChildWorkspaceMode::ExplicitCustomWorkspace => {
            String::from("explicit_custom_workspace")
        }
    }
}

fn planner_phase_command(
    command: &RunTurnCommand,
    phase: &str,
    loop_iteration: u32,
) -> RunTurnCommand {
    let mut command = command.clone();
    command.cancellation_id =
        planner_phase_cancellation_id(&command.cancellation_id, phase, loop_iteration);
    if command.options.get("mock_scenario").and_then(Value::as_str) == Some("planner_worker")
        && let Some(options) = command.options.as_object_mut()
    {
        options.insert(
            String::from("mock_planner_phase"),
            Value::String(phase.to_owned()),
        );
        options.insert(
            String::from("mock_planner_iteration"),
            Value::String(loop_iteration.to_string()),
        );
    }
    command
}

fn provider_visible_text(events: &[ProviderEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            ProviderEvent::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn parse_planner_tasks(
    events: &[ProviderEvent],
    max_children: u32,
) -> Result<Vec<PlannedTask>, RunTurnError> {
    let value: Value = serde_json::from_str(&provider_visible_text(events))
        .map_err(|_| RunTurnError::PlannerOutputInvalid)?;
    let tasks = value
        .get("tasks")
        .and_then(Value::as_array)
        .ok_or(RunTurnError::PlannerOutputInvalid)?;
    if tasks.len() < 2 || u32::try_from(tasks.len()).map_or(true, |count| count > max_children) {
        return Err(RunTurnError::PlannerOutputInvalid);
    }
    let mut ids = BTreeSet::new();
    tasks
        .iter()
        .map(|task| {
            let task_id = task
                .get("task_id")
                .or_else(|| task.get("id"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty() && value.len() <= 256)
                .ok_or(RunTurnError::PlannerOutputInvalid)?;
            let description = task
                .get("description")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty() && value.len() <= 64 * 1024)
                .ok_or(RunTurnError::PlannerOutputInvalid)?;
            if !ids.insert(task_id.to_owned()) {
                return Err(RunTurnError::PlannerOutputInvalid);
            }
            Ok(PlannedTask {
                task_id: task_id.to_owned(),
                description: description.to_owned(),
            })
        })
        .collect()
}

fn parse_reviewer_findings(
    events: &[ProviderEvent],
    tasks: &BTreeMap<String, PlannedTask>,
) -> Result<(bool, Vec<String>, Vec<String>), RunTurnError> {
    let value: Value = serde_json::from_str(&provider_visible_text(events))
        .map_err(|_| RunTurnError::ReviewerOutputInvalid)?;
    let approved = value
        .get("approved")
        .and_then(Value::as_bool)
        .ok_or(RunTurnError::ReviewerOutputInvalid)?;
    let rejected_task_ids = value
        .get("rejected_task_ids")
        .and_then(Value::as_array)
        .ok_or(RunTurnError::ReviewerOutputInvalid)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|task_id| tasks.contains_key(*task_id))
                .map(str::to_owned)
                .ok_or(RunTurnError::ReviewerOutputInvalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let findings = value
        .get("findings")
        .and_then(Value::as_array)
        .ok_or(RunTurnError::ReviewerOutputInvalid)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|finding| !finding.trim().is_empty() && finding.len() <= 64 * 1024)
                .map(str::to_owned)
                .ok_or(RunTurnError::ReviewerOutputInvalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut unique = rejected_task_ids.clone();
    unique.sort();
    unique.dedup();
    if approved != rejected_task_ids.is_empty()
        || unique.len() != rejected_task_ids.len()
        || findings.is_empty()
    {
        return Err(RunTurnError::ReviewerOutputInvalid);
    }
    Ok((approved, rejected_task_ids, findings))
}

fn planner_tasks_for_iteration(
    state: &crate::session::SessionState,
    loop_iteration: u32,
) -> Result<Vec<PlannedTask>, RunTurnError> {
    if loop_iteration == 0 {
        return Ok(state.planner_worker.tasks.values().cloned().collect());
    }
    let review = state
        .planner_worker
        .reviews
        .iter()
        .find(|review| review.loop_iteration.checked_add(1) == Some(loop_iteration))
        .ok_or(RunTurnError::PlannerOutputInvalid)?;
    review
        .rejected_task_ids
        .iter()
        .map(|task_id| {
            state
                .planner_worker
                .tasks
                .get(task_id)
                .cloned()
                .ok_or(RunTurnError::PlannerOutputInvalid)
        })
        .collect()
}

fn assistant_already_committed(
    state: &crate::session::SessionState,
    cancellation_id: &str,
    events: &[ProviderEvent],
) -> bool {
    let expected = provider_visible_text(events);
    state.conversation.history().iter().any(|entry| {
        matches!(
            entry,
            ConversationEntry::AssistantMessage(message)
                if message.id.0.ends_with(&format!(":{cancellation_id}"))
                    && message.text == expected
        )
    })
}

fn latest_assistant_summary(
    state: &crate::session::SessionState,
    events: &[ProviderEvent],
) -> String {
    let summary = state
        .conversation
        .history()
        .iter()
        .rev()
        .find_map(|entry| match entry {
            ConversationEntry::AssistantMessage(message) => Some(message.text.clone()),
            _ => None,
        })
        .unwrap_or_else(|| provider_visible_text(events));
    summary.chars().take(256 * 1024).collect()
}

fn research_completion_after(options: &Value, limit: u32) -> Result<u32, RunTurnError> {
    let selected = options
        .get("research_complete_after")
        .map_or(Some(3), |value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
                .and_then(|value| u32::try_from(value).ok())
        })
        .filter(|value| *value > 0 && *value <= limit)
        .ok_or(RunTurnError::InvalidResearchCompletionCriterion)?;
    Ok(selected)
}

fn next_style_step(state: &crate::session::SessionState) -> u64 {
    state.style_execution.as_ref().map_or(1, |execution| {
        execution
            .completed_nodes
            .iter()
            .map(|node| node.step)
            .chain(execution.failed_nodes.iter().map(|node| node.step))
            .chain(
                execution
                    .transitions
                    .iter()
                    .map(|transition| transition.step),
            )
            .max()
            .unwrap_or(0)
            .saturating_add(1)
    })
}

fn style_transition_variables(completed: &StyleNodeCompletedEvent) -> Result<Value, RunTurnError> {
    match completed.result_reference.as_deref() {
        Some("completion:criteria_met:true") => Ok(json!({"completion":{"criteria_met":true}})),
        Some("completion:criteria_met:false") => Ok(json!({"completion":{"criteria_met":false}})),
        Some(value) if value.starts_with("declarative-request:approval:true:") => {
            Ok(json!({"request":{"requires_approval":true}}))
        }
        Some(value) if value.starts_with("declarative-request:approval:false:") => {
            Ok(json!({"request":{"requires_approval":false}}))
        }
        Some("iteration:remaining:true") => Ok(json!({"iteration":{"remaining":true}})),
        Some("iteration:remaining:false") => Ok(json!({"iteration":{"remaining":false}})),
        Some("review:approved:true") => Ok(json!({"review":{"approved":true}})),
        Some("review:approved:false") => Ok(json!({"review":{"approved":false}})),
        Some(value) if value.starts_with("completion:criteria_met:") => {
            Err(RunTurnError::StyleGraphMismatch)
        }
        Some(value)
            if value.starts_with("declarative-request:")
                || value.starts_with("iteration:remaining:") =>
        {
            Err(RunTurnError::StyleGraphMismatch)
        }
        Some(value) if value.starts_with("review:approved:") => {
            Err(RunTurnError::StyleGraphMismatch)
        }
        _ => Ok(json!({})),
    }
}

fn declarative_inputs(options: &Value) -> Result<(bool, u32, Value, String), RunTurnError> {
    let requires_approval = options
        .get("graph_requires_approval")
        .map_or(Some(false), Value::as_bool)
        .ok_or(RunTurnError::InvalidDeclarativeInputs)?;
    let iteration_limit = options
        .get("graph_iterations")
        .map_or(Some(1), |value| {
            value.as_u64().and_then(|value| u32::try_from(value).ok())
        })
        .filter(|value| *value > 0)
        .ok_or(RunTurnError::InvalidDeclarativeInputs)?;
    let tool_arguments = options
        .get("graph_tool_arguments")
        .cloned()
        .unwrap_or_else(|| json!({"path":"README.md"}));
    if !tool_arguments.is_object() {
        return Err(RunTurnError::InvalidDeclarativeInputs);
    }
    let identity = canonical_json_bytes(&json!({
        "requires_approval": requires_approval,
        "iteration_limit": iteration_limit,
        "tool_arguments": tool_arguments,
    }))
    .map_err(map_projection_measure_error)?;
    let reference = format!(
        "declarative-request:approval:{requires_approval}:{}",
        ContentHash::digest(&identity)
    );
    Ok((
        requires_approval,
        iteration_limit,
        tool_arguments,
        reference,
    ))
}

fn validate_declarative_resume_request(
    state: &crate::session::SessionState,
    expected_reference: &str,
) -> Result<(), RunTurnError> {
    let Some(execution) = state.style_execution.as_ref() else {
        return Err(RunTurnError::StyleGraphMismatch);
    };
    if execution.input_reference.as_deref() != Some(expected_reference) {
        return Err(RunTurnError::ContextRecoveryIdentityMismatch);
    }
    if let Some(branch) = execution
        .completed_nodes
        .iter()
        .find(|node| node.node_id == "branch")
        && branch.result_reference.as_deref() != Some(expected_reference)
    {
        return Err(RunTurnError::ContextRecoveryIdentityMismatch);
    }
    Ok(())
}

fn validate_style_approval_cursor(
    execution: &ActiveStyleTurn,
    approval: &StyleApprovalContinuation,
    continuation_id: &ContinuationId,
) -> Result<(), RunTurnError> {
    if execution.current.directive != StyleNodeDirective::UserApproval
        || execution.current.id != approval.node_id
        || execution.attempt != approval.attempt
        || execution.loop_iteration != approval.loop_iteration
        || execution.step != approval.step
        || approval.request_reference.trim().is_empty()
        || continuation_id.to_string().trim().is_empty()
    {
        return Err(RunTurnError::InvalidContinuationPayload);
    }
    Ok(())
}

fn research_finding_bytes(
    goal: &str,
    loop_iteration: u32,
    events: &[ProviderEvent],
) -> Result<Vec<u8>, RunTurnError> {
    let finding: String = events
        .iter()
        .filter_map(|event| match event {
            ProviderEvent::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    let tool_calls = events
        .iter()
        .filter(|event| matches!(event, ProviderEvent::ToolProposed { .. }))
        .count();
    serde_json::to_vec(&json!({
        "schema_version": 1,
        "kind": "research_finding",
        "iteration": loop_iteration.saturating_add(1),
        "goal": goal,
        "finding": finding,
        "tool_calls": tool_calls,
    }))
    .map_err(|_| RunTurnError::ResearchArtifactEncoding)
}

fn recoverable_research_model_events(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Option<Vec<ProviderEvent>> {
    let evidence = state
        .style_execution
        .as_ref()?
        .latest_model_execution
        .as_ref()?;
    if evidence.cancellation_id != command.cancellation_id || evidence.completed_at.is_none() {
        return None;
    }
    if !evidence.response_completed && evidence.tool_proposals.is_empty() {
        return None;
    }
    let mut events = Vec::new();
    if !evidence.visible_text.is_empty() {
        events.push(ProviderEvent::Text(evidence.visible_text.clone()));
    }
    events.extend(
        evidence
            .tool_proposals
            .iter()
            .map(|proposal| ProviderEvent::ToolProposed {
                continuation_id: proposal.continuation_id.clone(),
                call_id: proposal.call_id.clone(),
                tool: proposal.tool.clone(),
                arguments: proposal.arguments.clone(),
            }),
    );
    Some(events)
}

fn validate_research_resume_request(
    state: &crate::session::SessionState,
    command: &RunTurnCommand,
) -> Result<(), RunTurnError> {
    let Some(execution) = state.style_execution.as_ref() else {
        return Err(RunTurnError::StyleGraphMismatch);
    };
    let Some(boundary) = execution
        .context_boundaries
        .iter()
        .find(|boundary| boundary.identity.run_id == command.cancellation_id)
    else {
        return Ok(());
    };
    let request_hash = current_context_request_hash(state, command)?;
    if boundary.identity.request_hash != request_hash {
        return Err(RunTurnError::ContextRecoveryIdentityMismatch);
    }
    Ok(())
}

fn research_base_run_id(cancellation_id: &str) -> Option<&str> {
    let (base, iteration) = cancellation_id.rsplit_once("-research-")?;
    (!base.is_empty()
        && iteration
            .parse::<u32>()
            .ok()
            .is_some_and(|iteration| iteration > 0))
    .then_some(base)
}

fn planner_base_run_id(cancellation_id: &str) -> Option<&str> {
    let (base_and_phase, iteration) = cancellation_id.rsplit_once('-')?;
    let (base, phase) = base_and_phase.rsplit_once("-planner-")?;
    (!base.is_empty()
        && matches!(phase, "plan" | "integrate" | "review")
        && iteration.parse::<u32>().is_ok())
    .then_some(base)
}

fn planner_base_run_id_from_state(state: &crate::session::SessionState) -> Option<&str> {
    let binding = state.style_binding.as_ref()?;
    let executor = CompiledStyleExecutor::from_binding(binding).ok()?;
    if executor.adapter_kind() != Some(StyleAdapterKind::PlannerWorkerReviewer) {
        return None;
    }
    state
        .style_execution
        .as_ref()?
        .context_boundaries
        .iter()
        .find(|boundary| {
            boundary.identity.boundary == "turn_start"
                && boundary.identity.origin == expected_initial_context_origin(state)
        })
        .map(|boundary| boundary.identity.run_id.as_str())
}

fn planner_phase_cancellation_id(base: &str, phase: &str, iteration: u32) -> String {
    let phase_discriminator = match phase {
        "plan" => 1_u128,
        "integrate" => 2_u128,
        "review" => 3_u128,
        _ => 4_u128,
    };
    uuid::Uuid::parse_str(base).map_or_else(
        |_| format!("{base}-planner-{phase}-{iteration}"),
        |base| {
            let discriminator = (phase_discriminator << 64) | u128::from(iteration);
            uuid::Uuid::from_u128(base.as_u128() ^ discriminator)
                .hyphenated()
                .to_string()
        },
    )
}

fn planner_child_cancellation_id(base: &str, task_id: &str, iteration: u32) -> String {
    uuid::Uuid::parse_str(base).map_or_else(
        |_| format!("{base}-child-{task_id}-{iteration}"),
        |base| {
            let digest = ContentHash::digest(format!("{task_id}:{iteration}").as_bytes());
            let mut discriminator = [0_u8; 16];
            discriminator.copy_from_slice(&digest.as_bytes()[..16]);
            uuid::Uuid::from_u128(
                base.as_u128() ^ u128::from_le_bytes(discriminator) ^ u128::from(iteration),
            )
            .hyphenated()
            .to_string()
        },
    )
}

fn research_base_run_id_from_state(state: &crate::session::SessionState) -> Option<&str> {
    let binding = state.style_binding.as_ref()?;
    let executor = CompiledStyleExecutor::from_binding(binding).ok()?;
    if executor.adapter_kind() != Some(StyleAdapterKind::ResearchLoop) {
        return None;
    }
    state
        .style_execution
        .as_ref()?
        .context_boundaries
        .iter()
        .find(|boundary| {
            boundary.identity.boundary == "turn_start"
                && boundary.identity.origin == expected_initial_context_origin(state)
        })
        .map(|boundary| boundary.identity.run_id.as_str())
}

fn research_iteration_cancellation_id(base: &str, iteration: u32) -> String {
    uuid::Uuid::parse_str(base).map_or_else(
        |_| format!("{base}-research-{iteration}"),
        |base| {
            uuid::Uuid::from_u128(base.as_u128() ^ u128::from(iteration))
                .hyphenated()
                .to_string()
        },
    )
}

fn research_assistant_committed(
    state: &crate::session::SessionState,
    cancellation_id: &str,
    events: &[ProviderEvent],
) -> bool {
    let visible: String = events
        .iter()
        .filter_map(|event| match event {
            ProviderEvent::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    visible.is_empty()
        || state.conversation.history().iter().rev().any(|entry| {
            matches!(
                entry,
                ConversationEntry::AssistantMessage(assistant)
                    if assistant.id.0.ends_with(&format!(":{cancellation_id}"))
                        && assistant.text == visible
            )
        })
}

const fn active_graph_state_is_outside_projection() -> bool {
    false
}

#[derive(Debug, Error)]
pub enum RunTurnError {
    #[error("turn request is invalid")]
    Invalid,
    #[error("session identifier is invalid")]
    InvalidSession,
    #[error("the session predates immutable style execution and requires migration")]
    StyleMigrationRequired,
    #[error("the immutable compiled style binding is invalid")]
    StyleBindingInvalid,
    #[error("the style selects plugins but no runtime plugin composer is configured")]
    PluginCompositionUnavailable,
    #[error("style-selected plugin composition failed: {0}")]
    PluginComposition(PluginCompositionError),
    #[error("compiled session-style execution failed: {0}")]
    StyleExecutor(StyleExecutorError),
    #[error("the compiled session graph does not match canonical execution state")]
    StyleGraphMismatch,
    #[error("style node `{0}` requires explicit recovery before execution may continue")]
    StyleRecoveryRequired(String),
    #[error(
        "style node `{node}` requires explicit recovery from control state `{phase}` before execution may continue"
    )]
    StyleControlRecoveryRequired { node: String, phase: &'static str },
    #[error("style execution is terminal")]
    StyleExecutionTerminal,
    #[error("style execution is terminal: {0}")]
    StyleExecutionTerminalReason(String),
    #[error("session style `{0}` is not supported by the live turn executor")]
    UnsupportedStyleExecution(String),
    #[error("style graph step budget {limit} is exhausted")]
    StyleStepBudgetExceeded { limit: u64 },
    #[error("expected style node kind `{expected}`, found node `{actual}`")]
    UnexpectedStyleNode {
        expected: &'static str,
        actual: String,
    },
    #[error("memory retrieval failed: {0}")]
    Memory(MemoryLogicError),
    #[error("memory selection bound cannot be represented")]
    MemoryBoundOverflow,
    #[error("memory scope `{0}` is not supported")]
    UnsupportedMemoryScope(String),
    #[error("memory scope `{0}` has no runtime identity")]
    MemoryScopeIdentityUnavailable(String),
    #[error("memory query construction is invalid")]
    InvalidMemoryQueryConfiguration,
    #[error("memory query construction requires a session goal")]
    MemorySessionGoalUnavailable,
    #[error("memory query construction requires an explicit graph/client query")]
    ExplicitMemoryQueryRequired,
    #[error("memory injection requires the current user input")]
    CurrentInputMissing,
    #[error("memory injection location `{0}` is unsupported")]
    UnsupportedMemoryInjection(String),
    #[error("memory retrieval timing `{0}` has no live runtime lifecycle hook")]
    UnsupportedMemoryRetrievalTiming(String),
    #[error("context recovery run identity does not match the active canonical turn")]
    ContextRecoveryIdentityMismatch,
    #[error("context phase `{0}` may have invoked a blocking interceptor and requires a receipt")]
    AmbiguousContextPhase(String),
    #[error("provider resume may have crossed the proposal or dispatch boundary")]
    AmbiguousProviderResume,
    #[error("context-artifact memory injection requires an approved immutable artifact")]
    MemoryContextArtifactRequired,
    #[error("style provider token usage overflowed")]
    StyleTokenUsageOverflow,
    #[error("compaction failed: {0}")]
    Compaction(CompactionError),
    #[error("summary compaction requires an approved typed summary")]
    ApprovedSummaryRequired,
    #[error("artifact-handoff compaction requires an approved immutable artifact")]
    ApprovedArtifactHandoffRequired,
    #[error("compaction strategy `{0}` is not supported by the live adapter")]
    UnsupportedCompactionStrategy(String),
    #[error("provider projection budget is invalid")]
    InvalidProjectionBudget,
    #[error("provider projection size cannot be represented")]
    ProjectionSizeOverflow,
    #[error(
        "provider projection is estimated at {estimated_tokens} tokens, exceeding the effective limit {limit}"
    )]
    ProviderProjectionLimitExceeded { estimated_tokens: u64, limit: u64 },
    #[error(
        "provider projection serialization is {serialized_bytes} bytes, exceeding the hard safety limit {limit}"
    )]
    ProviderProjectionByteLimitExceeded { serialized_bytes: u64, limit: u64 },
    #[error("compaction would discard required provider entry `{0}`")]
    CompactionPreservationViolation(String),
    #[error("an interceptor changed a context proposal into an incompatible action")]
    InvalidContextInterceptionReplacement,
    #[error("{operation} requires approval: {reason}")]
    ContextApprovalRequired {
        operation: &'static str,
        reason: String,
    },
    #[error("{operation} was rejected: {reason}")]
    ContextRejected {
        operation: &'static str,
        reason: String,
    },
    #[error("{0} returned an unsupported interceptor decision")]
    UnsupportedContextDecision(&'static str),
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
    #[error("terminal tool conversation for call `{0}` is missing or conflicts with its receipt")]
    ToolConversationRecoveryConflict(String),
    #[error("tool host returned an invalid artifact identifier")]
    InvalidArtifact,
    #[error("research completion criterion is invalid or exceeds the compiled loop bound")]
    InvalidResearchCompletionCriterion,
    #[error("declarative graph inputs are invalid or exceed the compiled loop bound")]
    InvalidDeclarativeInputs,
    #[error("runtime-managed child task input is required")]
    ChildTaskRequired,
    #[error("runtime-managed child task input does not match its canonical assignment")]
    ChildTaskMismatch,
    #[error("planner-worker child policy is invalid or unsupported")]
    InvalidChildPolicy,
    #[error("runtime-managed child sessions are not configured")]
    ChildSessionsUnavailable,
    #[error("child-agent depth exceeds the selected style bound")]
    ChildDepthExceeded,
    #[error("runtime-managed child session failed: {0}")]
    ChildSession(crate::child_session::ChildSessionLogicError),
    #[error("planner model output is not a valid bounded task plan")]
    PlannerOutputInvalid,
    #[error("reviewer model output is not a valid bounded decision")]
    ReviewerOutputInvalid,
    #[error("planner/reviewer model execution failed")]
    PlannerModelFailed,
    #[error("style-owned tool approval is not supported by this graph adapter")]
    StyleOwnedToolApprovalUnsupported,
    #[error("style-owned tool replacement is not supported by this graph adapter")]
    StyleOwnedToolReplacementUnsupported,
    #[error("research finding artifact could not be encoded")]
    ResearchArtifactEncoding,
    #[error("research artifact persistence failed: {0}")]
    ResearchArtifact(crate::artifact::ArtifactPersistenceError),
    #[error("durable tool receipt does not match dispatch {0}")]
    InvalidRecoveryReceipt(String),
    #[error(
        "provider execution failed ({provider}) and its audit event could not be committed ({audit})"
    )]
    ProviderFailureAudit { provider: String, audit: String },
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use agentmod_event_model::{EventClassification, EventMetadata, EventOrigin, EventScope};
    use agentmod_event_pipeline::{
        BlockingInterceptor, BlockingPipelineBuilder, Decision, FailurePolicy, InterceptorError,
        InterceptorRegistration, OrderingSpec,
    };
    use agentmod_primitives::{
        ByteCount, ContentHash, CorrelationId, EventId, TimestampMillis, Version,
    };
    use agentmod_runtime_data::{
        artifact::{
            ArtifactDataError, ArtifactDataPort, InspectArtifactDataRequest,
            PersistArtifactDataRequest, PersistedArtifactDataRecord,
        },
        continuation::{
            ContinuationDataError, ContinuationDataPort, ContinuationPayloadRecord,
            ContinuationRecord, ContinuationStateRecord, ContinuationWakeRecord,
            CreateContinuationDataRequest, ResolveContinuationDataRecord,
            ResolveContinuationDataRequest, StyleApprovalPayloadRecord, ToolApprovalPayloadRecord,
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
        memory::{
            MemoryDataError, MemoryDataPort, RetrieveMemoryDataRequest, RetrievedMemoryDataRecord,
            WriteMemoryDataRecord, WriteMemoryDataRequest,
        },
        tool::{ExecuteToolDataRequest, ToolDataError, ToolDataEvent, ToolDataPort},
    };
    use agentmod_session_style_sdk::BuiltInStyle;
    use serde_json::Value;
    use uuid::Uuid;

    use crate::{
        permission::{PermissionEffect, PermissionMatcher, PermissionPolicy, PermissionRule},
        session::{
            ApprovalRequestedEvent, ApprovalResolvedEvent, ChildSessionLinkedEvent,
            ModelRequestCancelledEvent, RuntimeCommittedEvent, SessionCreatedEvent,
            StyleExecutionInitializedEvent, StyleNodeCompletedEvent, StyleNodeEnteredEvent,
            StyleTransitionSelectedEvent, replay,
        },
        style_executor::tests::binding,
    };

    use super::*;

    struct ContextReplacingInterceptor;

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for ContextReplacingInterceptor {
        async fn intercept(
            &self,
            mut proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            proposal.origin = String::from("fixture-replacement");
            Ok(Decision::Replace(proposal))
        }
    }

    #[derive(Clone)]
    struct MockTurnData {
        state: Arc<MockTurnState>,
    }

    struct MockTurnState {
        events: StdMutex<Vec<EventEnvelope<Value>>>,
        harness_commands: StdMutex<Vec<HarnessDataCommand>>,
        harness_replies: StdMutex<VecDeque<Result<HarnessDataReply, HarnessDataError>>>,
        tool_reply: StdMutex<Result<Vec<ToolDataEvent>, ToolDataError>>,
        continuation: StdMutex<Option<ContinuationRecord>>,
        artifact_records: StdMutex<BTreeMap<String, PersistedArtifactDataRecord>>,
        artifact_persist_calls: AtomicU64,
        next_identity: AtomicU64,
    }

    impl MockTurnData {
        fn new(reply: Result<HarnessDataReply, HarnessDataError>) -> Self {
            Self {
                state: Arc::new(MockTurnState {
                    events: StdMutex::new(vec![created_event()]),
                    harness_commands: StdMutex::new(Vec::new()),
                    harness_replies: StdMutex::new(VecDeque::from([reply])),
                    tool_reply: StdMutex::new(Err(ToolDataError::Unavailable)),
                    continuation: StdMutex::new(None),
                    artifact_records: StdMutex::new(BTreeMap::new()),
                    artifact_persist_calls: AtomicU64::new(0),
                    next_identity: AtomicU64::new(100),
                }),
            }
        }

        fn with_events(
            reply: Result<HarnessDataReply, HarnessDataError>,
            events: Vec<EventEnvelope<Value>>,
        ) -> Self {
            let data = Self::new(reply);
            *data.state.events.lock().expect("events") = events;
            data
        }

        fn with_scenario(
            events: Vec<EventEnvelope<Value>>,
            harness_replies: Vec<Result<HarnessDataReply, HarnessDataError>>,
            tool_reply: Result<Vec<ToolDataEvent>, ToolDataError>,
        ) -> Self {
            let data = Self::new(Err(HarnessDataError::Unavailable));
            *data.state.events.lock().expect("events") = events;
            *data.state.harness_replies.lock().expect("replies") = harness_replies.into();
            *data.state.tool_reply.lock().expect("tool reply") = tool_reply;
            data
        }

        fn with_continuation(self, continuation: ContinuationRecord) -> Self {
            *self.state.continuation.lock().expect("continuation") = Some(continuation);
            self
        }

        fn with_artifact_records(
            self,
            records: BTreeMap<String, PersistedArtifactDataRecord>,
        ) -> Self {
            *self
                .state
                .artifact_records
                .lock()
                .expect("artifact records") = records;
            self
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

    impl MemoryDataPort for MockTurnData {
        fn write_memory(
            &self,
            request: WriteMemoryDataRequest,
        ) -> Result<WriteMemoryDataRecord, MemoryDataError> {
            Ok(WriteMemoryDataRecord {
                provider: request.provider,
                reference: String::from("ignored"),
                retained: false,
            })
        }

        fn retrieve_memory(
            &self,
            _request: RetrieveMemoryDataRequest,
        ) -> Result<Vec<RetrievedMemoryDataRecord>, MemoryDataError> {
            Ok(Vec::new())
        }
    }

    impl ArtifactDataPort for MockTurnData {
        fn persist_artifact(
            &self,
            request: PersistArtifactDataRequest,
        ) -> Result<PersistedArtifactDataRecord, ArtifactDataError> {
            self.state
                .artifact_persist_calls
                .fetch_add(1, Ordering::Relaxed);
            let hash = ContentHash::digest(&request.bytes).to_hex();
            let record = PersistedArtifactDataRecord {
                artifact_id: format!("blake3:{hash}"),
                artifact_reference: format!("artifact:blake3:{hash}"),
                mime_type: request.mime_type,
                byte_size: u64::try_from(request.bytes.len())
                    .map_err(|_| ArtifactDataError::InvalidRequest)?,
                creation_event: request.creation_event,
                producer: request.producer,
                content_hash: hash,
                deduplicated: false,
            };
            self.state
                .artifact_records
                .lock()
                .expect("artifact records")
                .insert(record.artifact_reference.clone(), record.clone());
            Ok(record)
        }

        fn inspect_artifact(
            &self,
            request: InspectArtifactDataRequest,
        ) -> Result<PersistedArtifactDataRecord, ArtifactDataError> {
            self.state
                .artifact_records
                .lock()
                .expect("artifact records")
                .get(&request.artifact_reference)
                .cloned()
                .ok_or(ArtifactDataError::NotFound)
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
            session_id: &str,
            id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            let record = self
                .state
                .continuation
                .lock()
                .expect("continuation")
                .clone()
                .expect("fixture continuation");
            assert_eq!(record.session_id, session_id);
            assert_eq!(record.id, id);
            Ok(record)
        }

        fn resolve(
            &self,
            request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            let mut continuation = self.state.continuation.lock().expect("continuation");
            let record = continuation.as_mut().expect("fixture continuation");
            assert_eq!(record.session_id, request.session_id);
            assert_eq!(record.id, request.id);
            let requested_state = if request.approved {
                ContinuationStateRecord::Resumed
            } else {
                ContinuationStateRecord::Cancelled
            };
            let transitioned = record.state == ContinuationStateRecord::Pending;
            if transitioned {
                record.state = requested_state;
            } else if record.state != requested_state {
                panic!("fixture continuation resolution conflict");
            }
            Ok(ResolveContinuationDataRecord {
                transitioned,
                state: record.state,
                payload: record.payload.clone(),
            })
        }
    }

    #[async_trait]
    impl ToolDataPort for MockTurnData {
        async fn execute_tool(
            &self,
            request: ExecuteToolDataRequest,
        ) -> Result<Vec<ToolDataEvent>, ToolDataError> {
            self.state
                .tool_reply
                .lock()
                .expect("tool reply")
                .clone()
                .map(|events| {
                    events
                        .into_iter()
                        .map(|event| match event {
                            ToolDataEvent::Started { call_id } if call_id == "__request__" => {
                                ToolDataEvent::Started {
                                    call_id: request.call_id.clone(),
                                }
                            }
                            ToolDataEvent::Completed {
                                call_id,
                                result,
                                artifact,
                                truncated,
                            } if call_id == "__request__" => ToolDataEvent::Completed {
                                call_id: request.call_id.clone(),
                                result,
                                artifact,
                                truncated,
                            },
                            other => other,
                        })
                        .collect()
                })
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
            self.state
                .harness_replies
                .lock()
                .expect("replies")
                .pop_front()
                .unwrap_or(Err(HarnessDataError::Unavailable))
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

    fn data_event(sequence: u64, payload: RuntimeCommittedEvent) -> EventEnvelope<Value> {
        let event_id = EventId::from_uuid(Uuid::from_u128(u128::from(sequence) + 1));
        let typed = EventEnvelope::seal(
            EventMetadata {
                event_id,
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(i64::try_from(sequence).expect("timestamp")),
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

    fn typed_event(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        let mapped = data_event(sequence, payload);
        EventEnvelope::seal(
            mapped.metadata,
            serde_json::from_value(mapped.payload).expect("typed payload"),
        )
        .expect("typed event")
    }

    fn created_event() -> EventEnvelope<Value> {
        data_event(
            1,
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: "fixture-workspace".into(),
                style: "persistent-chat".into(),
                style_binding: None,
            }),
        )
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

    fn tool_approval_policy() -> ProviderExecutionPolicy {
        let mut value = policy(PermissionEffect::Allow);
        value.user_policy = PermissionPolicy::new(
            "user",
            vec![PermissionRule {
                id: String::from("ask-tools"),
                priority: 100,
                matcher: PermissionMatcher {
                    action: Some(String::from("tool_call")),
                    ..PermissionMatcher::default()
                },
                effect: PermissionEffect::Ask,
                reason: String::from("fixture tool approval"),
            }],
            PermissionEffect::Allow,
            "allow non-tool actions",
        );
        value
    }

    #[tokio::test]
    async fn context_interceptor_replacement_fails_closed_until_typed_application_exists() {
        let mut style = BlockingPipelineBuilder::new();
        style.register(InterceptorRegistration::new(
            OrderingSpec::new("replace-context", "fixture"),
            std::time::Duration::from_secs(1),
            FailurePolicy::Abort,
            Arc::new(ContextReplacingInterceptor),
        ));
        let empty = Arc::new(
            BlockingPipelineBuilder::new()
                .compile()
                .expect("empty pipeline"),
        );
        let logic = TurnLogic::new(
            MockTurnData::new(Err(HarnessDataError::Unavailable)),
            ProviderExecutionPolicy {
                style_pipeline: Arc::new(style.compile().expect("style pipeline")),
                plugin_pipeline: empty,
                user_policy: PermissionPolicy::new(
                    "user",
                    vec![],
                    PermissionEffect::Allow,
                    "allow",
                ),
                mandatory_policy: PermissionPolicy::new(
                    "mandatory",
                    vec![],
                    PermissionEffect::Allow,
                    "allow",
                ),
            },
        );
        let proposal = ActionProposal {
            id: ProposalId(String::from("context")),
            action: ConsequentialAction::ContextConstruction {
                strategy: String::from("memory:file"),
            },
            style: String::from("persistent-chat"),
            workspace: String::from("workspace"),
            origin: String::from("runtime"),
        };
        assert!(matches!(
            logic
                .authorize_style_action(proposal, "memory retrieval")
                .await,
            Err(RunTurnError::InvalidContextInterceptionReplacement)
        ));
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

    fn persistent_binding(max_steps: u64) -> crate::session::SessionStyleBinding {
        let mut value = binding(BuiltInStyle::PersistentChat);
        value.budgets.max_steps = max_steps;
        value.memory.provider = String::from("none");
        value.memory.retrieval_timing = String::from("never");
        value.memory.injection_location = String::from("none");
        value.compaction.strategy = String::from("none");
        value.compaction.trigger_tokens = None;
        value
    }

    fn ephemeral_created_events() -> Vec<EventEnvelope<Value>> {
        let binding = binding(BuiltInStyle::EphemeralTurn);
        vec![data_event(
            1,
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: String::from("fixture-workspace"),
                style: binding.id.clone(),
                style_binding: Some(Box::new(binding)),
            }),
        )]
    }

    fn child_ephemeral_created_events(task: &str) -> Vec<EventEnvelope<Value>> {
        let binding = binding(BuiltInStyle::EphemeralTurn);
        vec![
            data_event(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture-workspace"),
                    style: binding.id.clone(),
                    style_binding: Some(Box::new(binding)),
                }),
            ),
            data_event(
                2,
                RuntimeCommittedEvent::ChildSessionLinked(ChildSessionLinkedEvent {
                    parent_session_id: SessionId::from_uuid(Uuid::from_u128(999)),
                    parent_action_sequence: Sequence::new(17).expect("parent sequence"),
                    parent_graph_node_id: String::from("spawn-workers"),
                    task_id: String::from("task-1"),
                    revision: 0,
                    depth: 1,
                    task: task.to_owned(),
                    input_hash: ContentHash::digest(task.as_bytes()),
                    token_budget: 10_000,
                }),
            ),
        ]
    }

    fn research_created_events() -> Vec<EventEnvelope<Value>> {
        let mut binding = binding(BuiltInStyle::ResearchLoop);
        binding.budgets.max_steps = 100;
        vec![data_event(
            1,
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: String::from("fixture-workspace"),
                style: binding.id.clone(),
                style_binding: Some(Box::new(binding)),
            }),
        )]
    }

    fn declarative_created_events() -> Vec<EventEnvelope<Value>> {
        let binding = binding(BuiltInStyle::DeclarativeGraph);
        vec![data_event(
            1,
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: String::from("fixture-workspace"),
                style: binding.id.clone(),
                style_binding: Some(Box::new(binding)),
            }),
        )]
    }

    fn style_tool_reply() -> Vec<ToolDataEvent> {
        vec![
            ToolDataEvent::Started {
                call_id: String::from("__request__"),
            },
            ToolDataEvent::Completed {
                call_id: String::from("__request__"),
                result: json!({"content":"fixture"}),
                artifact: None,
                truncated: false,
            },
        ]
    }

    fn install_style_approval_continuation(
        data: &MockTurnData,
        request: &RunTurnCommand,
        continuation_id: &str,
    ) {
        let pending_state = load_mock_state(data);
        let binding = pending_state.style_binding.as_ref().expect("style binding");
        let active = pending_state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.active_node.as_ref())
            .expect("active approval");
        let (_, _, _, request_reference) =
            declarative_inputs(&request.options).expect("declarative inputs");
        *data.state.continuation.lock().expect("continuation") = Some(ContinuationRecord {
            session_id: session_id().to_string(),
            id: continuation_id.to_owned(),
            state: ContinuationStateRecord::Pending,
            wake_condition: ContinuationWakeRecord::Manual,
            payload: ContinuationPayloadRecord::StyleApproval(Box::new(
                StyleApprovalPayloadRecord {
                    session_id: session_id().to_string(),
                    workspace: String::from("fixture-workspace"),
                    prompt: request.prompt.clone(),
                    provider: request.provider.clone(),
                    model: request.model.clone(),
                    options: request.options.clone(),
                    style: String::from("declarative-graph"),
                    cancellation_id: request.cancellation_id.clone(),
                    compiled_style_cache_key: binding.compiled_cache_key.to_string(),
                    node_id: active.node_id.clone(),
                    attempt: active.attempt,
                    loop_iteration: active.loop_iteration,
                    step: active.step,
                    request_reference,
                },
            )),
            expires_at_millis: None,
        });
    }

    fn successful_harness_reply(text: &str) -> HarnessDataReply {
        HarnessDataReply::Events(vec![
            HarnessDataEvent::Started,
            HarnessDataEvent::Text(text.to_owned()),
            HarnessDataEvent::Completed {
                reason: String::from("stop"),
                input_tokens: 4,
                output_tokens: 1,
            },
        ])
    }

    #[tokio::test]
    async fn research_loop_persists_three_findings_and_completes_deterministically() {
        let data = MockTurnData::with_scenario(
            research_created_events(),
            vec![
                Ok(successful_harness_reply("finding one")),
                Ok(successful_harness_reply("finding two")),
                Ok(successful_harness_reply("finding three")),
            ],
            Err(ToolDataError::Unavailable),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));
        let mut request = command();
        request.options = json!({"research_complete_after":3});

        let result = logic.run_turn(request).await.expect("research loop");
        let state = load_mock_state(&data);
        let execution = state.style_execution.expect("style execution");

        assert_eq!(state.lifecycle, crate::session::SessionLifecycle::Completed);
        assert_eq!(state.artifact_persistences.len(), 3);
        assert!(state.artifact_persistences.values().all(|record| {
            record.state == crate::session::ArtifactPersistenceState::Completed
                && record.artifact_reference.is_some()
        }));
        assert_eq!(
            execution
                .completed_nodes
                .iter()
                .filter(|node| node.node_id == "persist")
                .count(),
            3
        );
        assert_eq!(
            execution
                .completed_nodes
                .iter()
                .filter(|node| node.node_id == "repeat")
                .map(|node| node.loop_iteration)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(
            execution.termination_reason.as_deref(),
            Some("complete_session")
        );
        assert_eq!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .iter()
                .filter(|command| matches!(command, HarnessDataCommand::Execute { .. }))
                .count(),
            3
        );
        assert_eq!(result.events.len(), 9);
    }

    #[tokio::test]
    async fn declarative_graph_executes_branch_three_tools_loop_and_terminal() {
        let data = MockTurnData::with_scenario(
            declarative_created_events(),
            Vec::new(),
            Ok(style_tool_reply()),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));
        let mut request = command();
        request.options = json!({
            "graph_requires_approval": false,
            "graph_iterations": 3,
            "graph_tool_arguments": {"path":"README.md"}
        });

        logic.run_turn(request).await.expect("declarative graph");
        let state = load_mock_state(&data);
        let execution = state.style_execution.expect("style execution");

        assert_eq!(state.lifecycle, crate::session::SessionLifecycle::Completed);
        assert_eq!(state.tool_executions.len(), 3);
        assert_eq!(
            execution
                .completed_nodes
                .iter()
                .filter(|node| node.node_id == "repeat")
                .map(|node| node.loop_iteration)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(
            execution.termination_reason.as_deref(),
            Some("complete_session")
        );
        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty(),
            "declarative graph does not invent provider calls"
        );
    }

    #[tokio::test]
    async fn declarative_graph_user_approval_resumes_once_into_tool_and_terminal() {
        let data = MockTurnData::with_scenario(
            declarative_created_events(),
            Vec::new(),
            Ok(style_tool_reply()),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));
        let mut request = command();
        request.options = json!({
            "graph_requires_approval": true,
            "graph_iterations": 1,
            "graph_tool_arguments": {"path":"README.md"}
        });
        let awaiting = logic
            .run_turn(request.clone())
            .await
            .expect("request graph approval");
        let continuation_id = awaiting
            .awaiting_continuation
            .expect("approval continuation");
        install_style_approval_continuation(&data, &request, &continuation_id);

        let resolved = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.clone(),
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect("resume graph approval");
        assert!(resolved.transitioned);
        assert!(resolved.awaiting_continuation.is_none());
        let state = load_mock_state(&data);
        assert_eq!(state.lifecycle, crate::session::SessionLifecycle::Completed);
        assert_eq!(state.tool_executions.len(), 1);
        assert_eq!(
            state.approvals[&ContinuationId::from_str(&continuation_id).expect("id")].state,
            ApprovalState::Approved
        );

        let repeated = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id,
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect("idempotent approval");
        assert!(!repeated.transitioned);
        assert_eq!(load_mock_state(&data).tool_executions.len(), 1);
    }

    #[tokio::test]
    async fn declarative_graph_rejects_changed_inputs_while_approval_is_pending() {
        let data = MockTurnData::with_scenario(
            declarative_created_events(),
            Vec::new(),
            Ok(style_tool_reply()),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));
        let mut request = command();
        request.options = json!({
            "graph_requires_approval": true,
            "graph_iterations": 1
        });
        logic
            .run_turn(request.clone())
            .await
            .expect("request graph approval");
        let before = data.event_types();
        request.options = json!({
            "graph_requires_approval": true,
            "graph_iterations": 2
        });

        let error = logic.run_turn(request).await.expect_err("changed inputs");

        assert!(matches!(
            error,
            RunTurnError::ContextRecoveryIdentityMismatch
        ));
        assert_eq!(data.event_types(), before);
        assert!(load_mock_state(&data).tool_executions.is_empty());
    }

    #[tokio::test]
    async fn declarative_graph_denial_fails_session_without_tool_dispatch() {
        let data = MockTurnData::with_scenario(
            declarative_created_events(),
            Vec::new(),
            Ok(style_tool_reply()),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));
        let mut request = command();
        request.options = json!({
            "graph_requires_approval": true,
            "graph_iterations": 1
        });
        let awaiting = logic
            .run_turn(request.clone())
            .await
            .expect("request graph approval");
        let continuation_id = awaiting
            .awaiting_continuation
            .expect("approval continuation");
        install_style_approval_continuation(&data, &request, &continuation_id);

        let denied = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.clone(),
                approved: false,
                resume_after_resolution: false,
            })
            .await
            .expect("deny graph approval");

        assert!(denied.transitioned);
        let state = load_mock_state(&data);
        assert_eq!(state.lifecycle, crate::session::SessionLifecycle::Failed);
        assert!(state.tool_executions.is_empty());
        assert_eq!(
            state
                .style_execution
                .as_ref()
                .and_then(|execution| execution.termination_reason.as_deref()),
            Some("declarative_approval_denied")
        );
        let repeated = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id,
                approved: false,
                resume_after_resolution: false,
            })
            .await
            .expect("idempotent graph denial");
        assert!(!repeated.transitioned);
        assert!(load_mock_state(&data).tool_executions.is_empty());
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the crash-cut matrix proves each durable artifact outbox phase has distinct recovery behavior"
    )]
    async fn research_artifact_crash_cuts_recover_without_ambiguous_redispatch() {
        let seed = MockTurnData::with_scenario(
            research_created_events(),
            vec![Ok(successful_harness_reply("recoverable finding"))],
            Err(ToolDataError::Unavailable),
        );
        let mut request = command();
        request.options = json!({"research_complete_after":1});
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("seed research turn");
        let full = seed.state.events.lock().expect("events").clone();
        let stored = seed
            .state
            .artifact_records
            .lock()
            .expect("artifact records")
            .clone();
        assert_eq!(stored.len(), 1);

        let assistant_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "conversation.entry_committed"
                    && event.payload.to_string().contains("assistant_message")
            })
            .expect("assistant event");
        let assistant_recovery = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=assistant_cut].to_vec(),
        );
        TurnLogic::new(assistant_recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("recover after assistant");
        assert_eq!(
            assistant_recovery
                .state
                .artifact_persist_calls
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            assistant_recovery
                .state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );

        let proposed_cut = full
            .iter()
            .position(|event| event.metadata.event_type == "artifact.persistence_proposed")
            .expect("proposed event");
        let proposed_recovery = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=proposed_cut].to_vec(),
        );
        TurnLogic::new(proposed_recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("re-evaluate proposal policy before any dispatch");
        assert_eq!(
            proposed_recovery
                .state
                .artifact_persist_calls
                .load(Ordering::Relaxed),
            1
        );

        let approved_cut = full
            .iter()
            .position(|event| event.metadata.event_type == "artifact.persistence_approved")
            .expect("approved event");
        let approved_recovery = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=approved_cut].to_vec(),
        );
        TurnLogic::new(approved_recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("dispatch approved artifact");
        assert_eq!(
            approved_recovery
                .state
                .artifact_persist_calls
                .load(Ordering::Relaxed),
            1
        );

        let dispatched_cut = full
            .iter()
            .position(|event| event.metadata.event_type == "artifact.persistence_dispatched")
            .expect("dispatched event");
        let dispatched_recovery = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=dispatched_cut].to_vec(),
        )
        .with_artifact_records(stored.clone());
        TurnLogic::new(dispatched_recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("reconcile dispatched artifact");
        assert_eq!(
            dispatched_recovery
                .state
                .artifact_persist_calls
                .load(Ordering::Relaxed),
            0,
            "present immutable receipt must not be written again"
        );

        let completed_cut = full
            .iter()
            .position(|event| event.metadata.event_type == "artifact.persistence_completed")
            .expect("completed event");
        let completed_recovery = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=completed_cut].to_vec(),
        )
        .with_artifact_records(stored);
        TurnLogic::new(completed_recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(request)
            .await
            .expect("complete artifact node from terminal event");
        assert_eq!(
            completed_recovery
                .state
                .artifact_persist_calls
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn research_loop_control_cuts_resume_terminal_state_without_effects() {
        let seed = MockTurnData::with_scenario(
            research_created_events(),
            vec![Ok(successful_harness_reply("terminal finding"))],
            Err(ToolDataError::Unavailable),
        );
        let mut request = command();
        request.options = json!({"research_complete_after":1});
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("seed research turn");
        let full = seed.state.events.lock().expect("events").clone();
        let stored = seed
            .state
            .artifact_records
            .lock()
            .expect("artifact records")
            .clone();
        let cuts = full
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                let payload = event.payload.to_string();
                ((event.metadata.event_type == "style.node_completed"
                    && (payload.contains("\"repeat\"") || payload.contains("\"done\"")))
                    || (event.metadata.event_type == "style.transition_selected"
                        && payload.contains("\"done\""))
                    || (event.metadata.event_type == "style.node_entered"
                        && payload.contains("\"done\"")))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(cuts.len(), 4);

        for cut in cuts {
            let recovery = MockTurnData::with_events(
                Err(HarnessDataError::Unavailable),
                full[..=cut].to_vec(),
            )
            .with_artifact_records(stored.clone());
            TurnLogic::new(recovery.clone(), policy(PermissionEffect::Allow))
                .run_turn(request.clone())
                .await
                .expect("recover loop control cut");
            assert!(
                recovery
                    .state
                    .harness_commands
                    .lock()
                    .expect("commands")
                    .is_empty()
            );
            assert_eq!(
                recovery
                    .state
                    .artifact_persist_calls
                    .load(Ordering::Relaxed),
                0
            );
            let state = load_mock_state(&recovery);
            assert_eq!(state.lifecycle, crate::session::SessionLifecycle::Completed);
            assert_eq!(
                state
                    .style_execution
                    .expect("execution")
                    .termination_reason
                    .as_deref(),
                Some("complete_session")
            );
        }
    }

    #[tokio::test]
    async fn research_resume_rejects_changed_request_configuration() {
        let seed = MockTurnData::with_scenario(
            research_created_events(),
            vec![
                Ok(successful_harness_reply("finding one")),
                Ok(successful_harness_reply("finding two")),
                Ok(successful_harness_reply("finding three")),
            ],
            Err(ToolDataError::Unavailable),
        );
        let mut original = command();
        original.options = json!({"research_complete_after":3});
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(original)
            .await
            .expect("seed research turn");
        let full = seed.state.events.lock().expect("events").clone();
        let cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "style.node_entered"
                    && event.payload.to_string().contains("\"repeat\"")
            })
            .expect("first loop entry");
        let recovery =
            MockTurnData::with_events(Err(HarnessDataError::Unavailable), full[..=cut].to_vec());
        let mut changed = command();
        changed.options = json!({"research_complete_after":1});

        let error = TurnLogic::new(recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(changed)
            .await
            .expect_err("changed recovery request");

        assert!(matches!(
            error,
            RunTurnError::ContextRecoveryIdentityMismatch
        ));
        assert!(
            recovery
                .state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
        assert_eq!(
            recovery
                .state
                .artifact_persist_calls
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the two crash cuts share one uninterrupted hash oracle and prove proposal and post-tool reconstruction together"
    )]
    async fn research_tool_crash_cuts_preserve_exact_finding_and_resume_canonical_call() {
        let initial = HarnessDataReply::Events(vec![
            HarnessDataEvent::Started,
            HarnessDataEvent::Text(String::from("investigated ")),
            HarnessDataEvent::ToolProposed {
                continuation_id: String::from("research-continue"),
                call_id: String::from("research-call"),
                tool: String::from("filesystem.read"),
                arguments: json!({"path":"README.md"}),
            },
        ]);
        let final_reply = successful_harness_reply("repository");
        let tool_reply = Ok(vec![
            ToolDataEvent::Started {
                call_id: String::from("research-call"),
            },
            ToolDataEvent::Completed {
                call_id: String::from("research-call"),
                result: json!({"content":"fixture"}),
                artifact: None,
                truncated: false,
            },
        ]);
        let seed = MockTurnData::with_scenario(
            research_created_events(),
            vec![Ok(initial), Ok(final_reply.clone())],
            tool_reply.clone(),
        );
        let mut request = command();
        request.options = json!({"research_complete_after":1});
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("seed research tool turn");
        let full = seed.state.events.lock().expect("events").clone();
        let expected_hash = seed
            .state
            .artifact_records
            .lock()
            .expect("artifact records")
            .values()
            .next()
            .expect("artifact")
            .content_hash
            .clone();

        let tool_entry_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "style.node_entered"
                    && event.payload.to_string().contains("\"tool\"")
            })
            .expect("tool entry");
        let tool_recovery = MockTurnData::with_scenario(
            full[..=tool_entry_cut].to_vec(),
            vec![Ok(final_reply)],
            tool_reply,
        );
        TurnLogic::new(tool_recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(request.clone())
            .await
            .expect("recover tool node");
        let recovered_hash = tool_recovery
            .state
            .artifact_records
            .lock()
            .expect("artifact records")
            .values()
            .next()
            .expect("recovered artifact")
            .content_hash
            .clone();
        assert_eq!(recovered_hash, expected_hash);
        assert_eq!(
            load_mock_state(&tool_recovery).tool_executions.len(),
            1,
            "canonical proposal executes one tool"
        );

        let persist_entry_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "style.node_entered"
                    && event.payload.to_string().contains("\"persist\"")
            })
            .expect("persist entry");
        let persist_recovery = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=persist_entry_cut].to_vec(),
        );
        TurnLogic::new(persist_recovery.clone(), policy(PermissionEffect::Allow))
            .run_turn(request)
            .await
            .expect("recover persist node");
        let recovered_hash = persist_recovery
            .state
            .artifact_records
            .lock()
            .expect("artifact records")
            .values()
            .next()
            .expect("recovered artifact")
            .content_hash
            .clone();
        assert_eq!(recovered_hash, expected_hash);
        assert!(
            persist_recovery
                .state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn research_tool_approval_resumes_artifact_loop_and_session_completion() {
        let data = MockTurnData::with_scenario(
            research_created_events(),
            vec![
                Ok(HarnessDataReply::Events(vec![
                    HarnessDataEvent::Started,
                    HarnessDataEvent::Text(String::from("approval finding ")),
                    HarnessDataEvent::ToolProposed {
                        continuation_id: String::from("research-continue"),
                        call_id: String::from("research-call"),
                        tool: String::from("filesystem.read"),
                        arguments: json!({"path":"README.md"}),
                    },
                ])),
                Ok(successful_harness_reply("approved")),
            ],
            Ok(vec![
                ToolDataEvent::Started {
                    call_id: String::from("research-call"),
                },
                ToolDataEvent::Completed {
                    call_id: String::from("research-call"),
                    result: json!({"content":"fixture"}),
                    artifact: None,
                    truncated: false,
                },
            ]),
        );
        let logic = TurnLogic::new(data.clone(), tool_approval_policy());
        let mut request = command();
        request.options = json!({"research_complete_after":1});
        let awaiting = logic
            .run_turn(request)
            .await
            .expect("request research tool approval");
        let continuation_id = awaiting
            .awaiting_continuation
            .expect("approval continuation");
        *data.state.continuation.lock().expect("continuation") = Some(ContinuationRecord {
            session_id: session_id().to_string(),
            id: continuation_id.clone(),
            state: ContinuationStateRecord::Pending,
            wake_condition: ContinuationWakeRecord::Manual,
            payload: ContinuationPayloadRecord::ToolApproval(Box::new(ToolApprovalPayloadRecord {
                session_id: session_id().to_string(),
                workspace: String::from("fixture-workspace"),
                call_id: String::from("research-call"),
                tool: String::from("filesystem.read"),
                arguments: json!({"path":"README.md"}),
                cancellation_id: String::from("cancel-1-research-1"),
                provider: String::from("deterministic-mock"),
                model: String::from("fixture"),
                options: json!({"research_complete_after":1}),
                style: String::from("research-loop"),
                harness_continuation: String::from("research-continue"),
                remaining_tool_calls: Vec::new(),
            })),
            expires_at_millis: None,
        });

        let resolved = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id,
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect("resume research approval");

        assert!(resolved.transitioned);
        assert!(resolved.awaiting_continuation.is_none());
        let state = load_mock_state(&data);
        assert_eq!(state.lifecycle, crate::session::SessionLifecycle::Completed);
        assert_eq!(state.artifact_persistences.len(), 1);
        assert_eq!(
            data.state.harness_commands.lock().expect("commands").len(),
            2
        );
    }

    fn load_mock_state(data: &MockTurnData) -> crate::session::SessionState {
        SessionPersistenceLogic::new(data.clone())
            .load_session(LoadSessionCommand {
                session_directory: PathBuf::from("sessions").join(session_id().to_string()),
                expected_session_id: session_id(),
            })
            .expect("replay fixture")
            .state
    }

    fn text_entry(id: &str, text: &str, sequence: u64) -> TextEntry {
        TextEntry {
            id: ConversationEntryId(id.into()),
            text: text.into(),
            source_sequence: Sequence::new(sequence).expect("sequence"),
        }
    }

    #[test]
    fn no_memory_projection_normalization_removes_only_retrieved_records() {
        let input = ConversationEntry::UserMessage(text_entry("user", "hello", 2));
        let memory = ConversationEntry::RetrievedMemory(RetrievedMemoryEntry {
            id: ConversationEntryId(String::from("memory")),
            provider: String::from("file"),
            query: String::from("hello"),
            scope: String::from("session:parent"),
            source: String::from("fixture"),
            reference: String::from("m1"),
            score: Some(1.0),
            content: String::from("parent-only memory"),
            injection_sequence: Sequence::new(3).expect("sequence"),
            injection_event: Some(EventId::from_uuid(Uuid::from_u128(30))),
            created_at_millis: 1,
            size_bytes: 18,
        });
        assert_eq!(
            projection_without_retrieved_memory(&[memory, input.clone()]),
            vec![input]
        );
    }

    #[test]
    fn projection_estimate_includes_serialized_provider_metadata() {
        let memory = ConversationEntry::RetrievedMemory(RetrievedMemoryEntry {
            id: ConversationEntryId(String::from("memory")),
            provider: String::from("file"),
            query: String::from("q"),
            scope: String::from("session:s1"),
            source: String::from("fixture"),
            reference: String::from("m1"),
            score: None,
            content: String::from("x"),
            injection_sequence: Sequence::FIRST,
            injection_event: None,
            created_at_millis: 1,
            size_bytes: 1,
        });
        let measure =
            measure_projection(std::slice::from_ref(&memory)).expect("projection estimate");
        assert!(
            measure.estimated_tokens > 1,
            "metadata and per-entry overhead must be counted"
        );
        assert!(
            measure.estimated_tokens < measure.serialized_bytes,
            "token pressure and serialized-byte safety use distinct units"
        );
        assert!(
            serialized_entry_contribution(&memory).expect("entry bytes") > measure.estimated_tokens,
            "injection accounting includes provenance omitted from provider projection"
        );
    }

    #[test]
    fn hard_projection_byte_cap_is_independent_of_token_configuration() {
        let oversized = ConversationEntry::UserMessage(text_entry(
            "large",
            &"x".repeat(usize::try_from(MAX_PROVIDER_PROJECTION_BYTES).expect("bound") + 1),
            1,
        ));
        assert!(matches!(
            validate_projection_measure(&[oversized], None),
            Err(RunTurnError::ProviderProjectionByteLimitExceeded { .. })
        ));
    }

    #[test]
    fn reserved_tokens_reduce_the_effective_projection_limit() {
        let mut binding = persistent_binding(10);
        binding.compaction.max_provider_projection_tokens = 1_000;
        binding.compaction.reserved_context_tokens = 250;
        assert_eq!(
            effective_projection_limit(&binding.compaction).expect("valid"),
            Some(750)
        );
        binding.compaction.reserved_context_tokens = 1_000;
        assert!(matches!(
            effective_projection_limit(&binding.compaction),
            Err(RunTurnError::InvalidProjectionBudget)
        ));
    }

    #[test]
    fn compaction_preserves_declared_current_input_and_system_instructions() {
        let system = ConversationEntry::SystemInstruction(text_entry("system", "rules", 1));
        let old = ConversationEntry::UserMessage(text_entry("old", "old", 2));
        let current = ConversationEntry::UserMessage(text_entry("current", "current", 3));
        let source = vec![system.clone(), old, current.clone()];
        let restored = restore_required_projection_entries(
            &source,
            &[],
            &[
                String::from("system_instructions"),
                String::from("current_input"),
            ],
        );
        assert_eq!(restored, vec![system, current]);
        validate_projection_preservation(
            &source,
            &restored,
            &[
                String::from("system_instructions"),
                String::from("current_input"),
            ],
        )
        .expect("required entries retained");
    }

    fn bound_control_events(include_transition: bool, max_steps: u64) -> Vec<EventEnvelope<Value>> {
        let binding = persistent_binding(max_steps);
        let graph = CompiledStyleExecutor::from_binding(&binding)
            .expect("executor")
            .compiled()
            .graph
            .clone();
        let mut events = vec![
            data_event(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture-workspace"),
                    style: String::from("persistent-chat"),
                    style_binding: Some(Box::new(binding)),
                }),
            ),
            data_event(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(graph),
                        input_reference: None,
                    },
                )),
            ),
            data_event(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            data_event(
                4,
                RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    result_reference: None,
                    artifact_reference: None,
                }),
            ),
        ];
        if include_transition {
            events.push(data_event(
                5,
                RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                    from_node_id: String::from("respond"),
                    to_node_id: String::from("tool"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ));
        }
        events
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the recovery fixture spells out each canonical boundary and phase event"
    )]
    fn context_recovery_events(complete_memory_phase: bool) -> Vec<EventEnvelope<Value>> {
        let binding = persistent_binding(10);
        let graph = CompiledStyleExecutor::from_binding(&binding)
            .expect("executor")
            .compiled()
            .graph
            .clone();
        let request_hash = context_request_hash(
            &command(),
            Sequence::new(2).expect("sequence"),
            "inspect the repository",
        )
        .expect("request hash");
        let turn_boundary = ContextBoundaryIdentity {
            node_id: String::from("respond"),
            boundary: String::from("turn_start"),
            run_id: String::from("cancel-1"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash,
            source_head: Sequence::new(4).expect("sequence"),
        };
        let turn_phase = ContextPhaseIdentity {
            boundary: turn_boundary.clone(),
            phase: String::from("memory"),
        };
        let boundary = ContextBoundaryIdentity {
            node_id: String::from("respond"),
            boundary: String::from("before_model_request"),
            run_id: String::from("cancel-1"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash,
            source_head: Sequence::new(8).expect("sequence"),
        };
        let phase = ContextPhaseIdentity {
            boundary: boundary.clone(),
            phase: String::from("memory"),
        };
        let user = ConversationEntry::UserMessage(text_entry(
            "user:2:cancel-1",
            "inspect the repository",
            2,
        ));
        let mut events = vec![
            data_event(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture-workspace"),
                    style: String::from("persistent-chat"),
                    style_binding: Some(Box::new(binding)),
                }),
            ),
            data_event(
                2,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent {
                        entry: user.clone(),
                    },
                ),
            ),
            data_event(
                3,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(graph),
                        input_reference: None,
                    },
                )),
            ),
            data_event(
                4,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            data_event(
                5,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: turn_boundary.clone(),
                }),
            ),
            data_event(
                6,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: turn_phase.clone(),
                }),
            ),
            data_event(
                7,
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: turn_phase,
                }),
            ),
            {
                let measurement =
                    measure_projection(std::slice::from_ref(&user)).expect("projection");
                data_event(
                    8,
                    RuntimeCommittedEvent::ContextBoundaryCompleted(
                        ContextBoundaryCompletedEvent {
                            identity: turn_boundary,
                            projection_hash: measurement.projection_hash,
                            estimated_tokens: measurement.estimated_tokens,
                            serialized_bytes: measurement.serialized_bytes,
                        },
                    ),
                )
            },
            data_event(
                9,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: boundary,
                }),
            ),
            data_event(
                10,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: phase.clone(),
                }),
            ),
        ];
        if complete_memory_phase {
            events.push(data_event(
                11,
                RuntimeCommittedEvent::ContextProjectionReplaced(ContextProjectionReplacedEvent {
                    replacement: vec![user],
                    provenance: ProjectionProvenance {
                        projection_id: String::from("memory:cancel-1:11"),
                        source_range: None,
                        method: String::from("memory:none"),
                        committed_at: Sequence::new(11).expect("sequence"),
                        artifact_id: None,
                    },
                    context_phase: Some(phase),
                }),
            ));
        }
        events
    }

    #[tokio::test]
    async fn completed_context_phase_recovers_without_duplicate_user_or_replacement() {
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(vec![
                HarnessDataEvent::Started,
                HarnessDataEvent::Text(String::from("recovered")),
                HarnessDataEvent::Completed {
                    reason: String::from("stop"),
                    input_tokens: 1,
                    output_tokens: 1,
                },
            ])),
            context_recovery_events(true),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        logic.run_turn(command()).await.expect("recover context");

        let types = data.event_types();
        assert_eq!(
            types
                .iter()
                .filter(|event| event.as_str() == "context.projection_replaced")
                .count(),
            1
        );
        assert_eq!(
            types
                .iter()
                .filter(|event| event.as_str() == "conversation.entry_committed")
                .count(),
            2,
            "one original user and one recovered assistant"
        );
        assert_eq!(
            data.state.harness_commands.lock().expect("commands").len(),
            1
        );
    }

    #[tokio::test]
    async fn ephemeral_turns_use_one_fresh_projection_and_discard_provider_state() {
        let data = MockTurnData::with_scenario(
            ephemeral_created_events(),
            vec![
                Ok(successful_harness_reply("turn-one-secret-output")),
                Ok(successful_harness_reply("turn-two-output")),
            ],
            Err(ToolDataError::Unavailable),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        logic
            .run_turn(command())
            .await
            .expect("first ephemeral turn");
        let first = load_mock_state(&data);
        assert!(first.conversation.provider_projection().is_empty());
        assert_eq!(
            data.state
                .events
                .lock()
                .expect("events")
                .iter()
                .filter(|event| event
                    .payload
                    .to_string()
                    .contains("ephemeral_fresh_context"))
                .count(),
            1
        );

        let mut second = command();
        second.prompt = String::from("second input without inherited state");
        second.cancellation_id = String::from("cancel-2");
        logic
            .run_turn(second.clone())
            .await
            .expect("second ephemeral turn");

        let state = load_mock_state(&data);
        assert!(state.conversation.provider_projection().is_empty());
        assert_eq!(
            state
                .conversation
                .history()
                .iter()
                .filter(|entry| matches!(entry, ConversationEntry::UserMessage(_)))
                .count(),
            2
        );
        assert_eq!(
            data.state
                .events
                .lock()
                .expect("events")
                .iter()
                .filter(|event| event
                    .payload
                    .to_string()
                    .contains("ephemeral_fresh_context"))
                .count(),
            2,
            "each turn commits exactly one fresh-context replacement"
        );
        let commands = data.state.harness_commands.lock().expect("commands");
        assert_eq!(commands.len(), 2);
        let HarnessDataCommand::Execute { entries, .. } = &commands[1] else {
            panic!("execute command");
        };
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            entries.as_slice(),
            [agentmod_runtime_data::harness::HarnessDataEntry::User(value)]
                if value == &second.prompt
        ));
        assert!(
            !format!("{entries:?}").contains("turn-one-secret-output"),
            "discarded provider-visible output must not leak into the next turn"
        );
    }

    #[tokio::test]
    async fn child_task_uses_typed_pending_task_without_fabricating_user_history() {
        let task = String::from("inspect the exact scheduler recovery invariant");
        let data = MockTurnData::with_scenario(
            child_ephemeral_created_events(&task),
            vec![Ok(successful_harness_reply("worker result"))],
            Err(ToolDataError::Unavailable),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));
        let mut child_command = command();
        child_command.prompt = task.clone();

        let result = Box::pin(logic.run_child_task(child_command)).await;
        assert!(
            result.is_ok(),
            "child task failed: {result:?}; events: {:?}",
            data.event_types()
        );

        let state = load_mock_state(&data);
        assert!(
            state
                .conversation
                .history()
                .iter()
                .all(|entry| !matches!(entry, ConversationEntry::UserMessage(_)))
        );
        let commands = data.state.harness_commands.lock().expect("commands");
        let HarnessDataCommand::Execute { entries, .. } = &commands[0] else {
            panic!("execute command");
        };
        assert!(matches!(
            entries.as_slice(),
            [agentmod_runtime_data::harness::HarnessDataEntry::Metadata { key, value }]
                if key == "pending_task"
                    && value["id"] == "task-1"
                    && value["description"] == task
        ));
    }

    #[tokio::test]
    async fn ephemeral_context_kill_cuts_resume_without_duplicate_effects() {
        let seed = MockTurnData::with_scenario(
            ephemeral_created_events(),
            vec![Ok(successful_harness_reply("seed-output"))],
            Err(ToolDataError::Unavailable),
        );
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(command())
            .await
            .expect("seed complete turn");
        let full = seed.state.events.lock().expect("events").clone();
        let cuts = [
            full.iter()
                .position(|event| {
                    event.metadata.event_type == "context.projection_replaced"
                        && event
                            .payload
                            .to_string()
                            .contains("ephemeral_fresh_context")
                })
                .expect("fresh replacement"),
            full.iter()
                .position(|event| {
                    event.metadata.event_type == "style.node_completed"
                        && event.payload.to_string().contains("fresh-context")
                })
                .expect("context node completion"),
            full.iter()
                .position(|event| {
                    event.metadata.event_type == "style.node_entered"
                        && event.payload.to_string().contains("\"respond\"")
                })
                .expect("model node entry"),
        ];

        for cut in cuts {
            let data = MockTurnData::with_events(
                Ok(successful_harness_reply("recovered-output")),
                full[..=cut].to_vec(),
            );
            TurnLogic::new(data.clone(), policy(PermissionEffect::Allow))
                .run_turn(command())
                .await
                .expect("recover exact context cut");

            assert_eq!(
                data.state.harness_commands.lock().expect("commands").len(),
                1,
                "recovery issues exactly the not-yet-proposed model request"
            );
            let state = load_mock_state(&data);
            assert!(state.conversation.provider_projection().is_empty());
            assert_eq!(
                state
                    .conversation
                    .history()
                    .iter()
                    .filter(|entry| matches!(entry, ConversationEntry::UserMessage(_)))
                    .count(),
                1
            );
            let events = data.state.events.lock().expect("events");
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event
                        .payload
                        .to_string()
                        .contains("ephemeral_fresh_context"))
                    .count(),
                1,
                "fresh replacement must not be duplicated at any recovery cut"
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.metadata.event_type == "model.request_proposed")
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the reducer fixture spells out both prohibited effect-evidence gaps"
    )]
    async fn ephemeral_nodes_cannot_complete_without_fresh_and_discard_evidence() {
        let binding = binding(BuiltInStyle::EphemeralTurn);
        let graph = CompiledStyleExecutor::from_binding(&binding)
            .expect("executor")
            .compiled()
            .graph
            .clone();
        let fresh_state = replay(&[
            typed_event(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture-workspace"),
                    style: binding.id.clone(),
                    style_binding: Some(Box::new(binding)),
                }),
            ),
            typed_event(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(graph),
                        input_reference: None,
                    },
                )),
            ),
            typed_event(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("fresh-context"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
        ])
        .expect("fresh active state");
        assert!(matches!(
            reduce(
                Some(fresh_state),
                &typed_event(
                    4,
                    RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                        node_id: String::from("fresh-context"),
                        attempt: 1,
                        loop_iteration: 0,
                        step: 1,
                        result_reference: None,
                        artifact_reference: None,
                    }),
                ),
            ),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));

        let seed = MockTurnData::with_scenario(
            ephemeral_created_events(),
            vec![Ok(successful_harness_reply("seed-output"))],
            Err(ToolDataError::Unavailable),
        );
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(command())
            .await
            .expect("seed turn");
        let full = seed.state.events.lock().expect("events").clone();
        let done_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "style.node_entered"
                    && event.payload.to_string().contains("\"done\"")
            })
            .expect("done entry");
        let truncated = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=done_cut].to_vec(),
        );
        let state = load_mock_state(&truncated);
        let active = state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.active_node.as_ref())
            .expect("active done")
            .clone();
        let next = state
            .last_sequence
            .checked_next()
            .expect("next sequence")
            .get();
        assert!(matches!(
            reduce(
                Some(state),
                &typed_event(
                    next,
                    RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                        node_id: active.node_id,
                        attempt: active.attempt,
                        loop_iteration: active.loop_iteration,
                        step: active.step,
                        result_reference: None,
                        artifact_reference: None,
                    }),
                ),
            ),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the recovery matrix keeps every terminal journal cut and request-identity mismatch visible"
    )]
    async fn ephemeral_terminal_cleanup_recovery_never_redispatches_model_or_duplicates_user() {
        let seed = MockTurnData::with_scenario(
            ephemeral_created_events(),
            vec![Ok(successful_harness_reply("seed-output"))],
            Err(ToolDataError::Unavailable),
        );
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(command())
            .await
            .expect("seed complete turn");
        let full = seed.state.events.lock().expect("events").clone();
        let pre_assistant_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "style.node_entered"
                    && event.payload.to_string().contains("\"done\"")
            })
            .expect("complete-turn entry");
        let assistant_cut = full
            .iter()
            .rposition(|event| event.metadata.event_type == "conversation.entry_committed")
            .expect("assistant entry");
        let discard_phase_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "context.phase_started"
                    && event.payload.to_string().contains("\"discard\"")
            })
            .expect("discard phase started");
        let discard_replacement_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "context.projection_replaced"
                    && event.payload.to_string().contains("ephemeral_discard")
            })
            .expect("discard replacement");
        let discard_boundary_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "context.boundary_completed"
                    && event.payload.to_string().contains("before_turn_completion")
            })
            .expect("discard boundary completed");

        for cut in [
            pre_assistant_cut,
            assistant_cut,
            discard_replacement_cut,
            discard_boundary_cut,
        ] {
            let data = MockTurnData::with_events(
                Err(HarnessDataError::Unavailable),
                full[..=cut].to_vec(),
            );
            let result = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow))
                .run_turn(command())
                .await
                .expect("recover terminal cleanup");
            assert!(result.events.is_empty());
            assert!(
                data.state
                    .harness_commands
                    .lock()
                    .expect("commands")
                    .is_empty(),
                "terminal cleanup recovery must not redispatch the model"
            );
            let state = load_mock_state(&data);
            assert!(state.conversation.provider_projection().is_empty());
            assert!(
                state
                    .style_execution
                    .expect("execution")
                    .active_node
                    .is_none()
            );
            assert_eq!(
                state
                    .conversation
                    .history()
                    .iter()
                    .filter(|entry| matches!(entry, ConversationEntry::UserMessage(_)))
                    .count(),
                1,
                "cleanup recovery must not commit a duplicate user turn"
            );
        }

        let ambiguous = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=discard_phase_cut].to_vec(),
        );
        let before = ambiguous.event_types();
        let error = TurnLogic::new(ambiguous.clone(), policy(PermissionEffect::Allow))
            .run_turn(command())
            .await
            .expect_err("started discard pipeline is ambiguous");
        assert!(matches!(
            error,
            RunTurnError::StyleRecoveryRequired(ref node) if node == "done"
        ));
        assert_eq!(ambiguous.event_types(), before);
        assert!(
            ambiguous
                .state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );

        let variants = [
            {
                let mut value = command();
                value.provider = String::from("different-provider");
                value
            },
            {
                let mut value = command();
                value.model = String::from("different-model");
                value
            },
            {
                let mut value = command();
                value.options = json!({"temperature":0.5});
                value
            },
        ];
        for cut in [pre_assistant_cut, assistant_cut, discard_boundary_cut] {
            for mismatched in variants.clone() {
                let data = MockTurnData::with_events(
                    Err(HarnessDataError::Unavailable),
                    full[..=cut].to_vec(),
                );
                let before = data.event_types();
                let error = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow))
                    .run_turn(mismatched)
                    .await
                    .expect_err("changed request identity must fail closed");
                assert!(matches!(error, RunTurnError::StyleRecoveryRequired(_)));
                assert_eq!(data.event_types(), before);
                assert!(
                    data.state
                        .harness_commands
                        .lock()
                        .expect("commands")
                        .is_empty()
                );
            }
        }
    }

    #[tokio::test]
    async fn ephemeral_zero_text_terminal_cuts_recover_without_redispatch() {
        let seed = MockTurnData::with_scenario(
            ephemeral_created_events(),
            vec![Ok(HarnessDataReply::Events(vec![
                HarnessDataEvent::Started,
                HarnessDataEvent::Completed {
                    reason: String::from("stop"),
                    input_tokens: 1,
                    output_tokens: 0,
                },
            ]))],
            Err(ToolDataError::Unavailable),
        );
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(command())
            .await
            .expect("seed zero-text turn");
        let full = seed.state.events.lock().expect("events").clone();
        let cuts = [
            full.iter()
                .position(|event| {
                    event.metadata.event_type == "style.node_entered"
                        && event.payload.to_string().contains("\"done\"")
                })
                .expect("complete-turn entry"),
            full.iter()
                .position(|event| {
                    event.metadata.event_type == "context.projection_replaced"
                        && event.payload.to_string().contains("ephemeral_discard")
                })
                .expect("discard replacement"),
            full.iter()
                .position(|event| {
                    event.metadata.event_type == "context.boundary_completed"
                        && event.payload.to_string().contains("before_turn_completion")
                })
                .expect("discard boundary"),
        ];
        for cut in cuts {
            let data = MockTurnData::with_events(
                Err(HarnessDataError::Unavailable),
                full[..=cut].to_vec(),
            );
            TurnLogic::new(data.clone(), policy(PermissionEffect::Allow))
                .run_turn(command())
                .await
                .expect("recover zero-text cut");
            assert!(
                data.state
                    .harness_commands
                    .lock()
                    .expect("commands")
                    .is_empty()
            );
            let state = load_mock_state(&data);
            assert!(state.conversation.provider_projection().is_empty());
            assert!(
                !state
                    .conversation
                    .history()
                    .iter()
                    .any(|entry| matches!(entry, ConversationEntry::AssistantMessage(_)))
            );
            assert!(
                state
                    .style_execution
                    .expect("execution")
                    .active_node
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn ephemeral_pre_assistant_recovery_concatenates_same_run_provider_exchanges() {
        let seed = MockTurnData::with_scenario(
            ephemeral_created_events(),
            vec![
                Ok(HarnessDataReply::Events(vec![
                    HarnessDataEvent::Started,
                    HarnessDataEvent::Text(String::from("initial ")),
                    HarnessDataEvent::ToolProposed {
                        continuation_id: String::from("continue-1"),
                        call_id: String::from("call-1"),
                        tool: String::from("filesystem.read"),
                        arguments: json!({"path":"README.md"}),
                    },
                ])),
                Ok(HarnessDataReply::Events(vec![
                    HarnessDataEvent::Started,
                    HarnessDataEvent::Text(String::from("final")),
                    HarnessDataEvent::Completed {
                        reason: String::from("stop"),
                        input_tokens: 2,
                        output_tokens: 2,
                    },
                ])),
            ],
            Ok(vec![
                ToolDataEvent::Started {
                    call_id: String::from("call-1"),
                },
                ToolDataEvent::Completed {
                    call_id: String::from("call-1"),
                    result: json!({"content":"fixture"}),
                    artifact: None,
                    truncated: false,
                },
            ]),
        );
        TurnLogic::new(seed.clone(), policy(PermissionEffect::Allow))
            .run_turn(command())
            .await
            .expect("seed multi-exchange turn");
        let full = seed.state.events.lock().expect("events").clone();
        let done_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "style.node_entered"
                    && event.payload.to_string().contains("\"done\"")
            })
            .expect("complete-turn entry");
        let data = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=done_cut].to_vec(),
        );

        TurnLogic::new(data.clone(), policy(PermissionEffect::Allow))
            .run_turn(command())
            .await
            .expect("recover multi-exchange assistant");

        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty(),
            "recovery must not redispatch either provider exchange"
        );
        let state = load_mock_state(&data);
        let assistants = state
            .conversation
            .history()
            .iter()
            .filter_map(|entry| match entry {
                ConversationEntry::AssistantMessage(assistant) => Some(assistant.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(assistants, ["initial final"]);
        assert!(state.conversation.provider_projection().is_empty());
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the fixture proves normal and crash-recovered approval completion use the identical turn aggregate"
    )]
    async fn ephemeral_approval_completion_uses_the_canonical_turn_text_aggregate() {
        let data = MockTurnData::with_scenario(
            ephemeral_created_events(),
            vec![
                Ok(HarnessDataReply::Events(vec![
                    HarnessDataEvent::Started,
                    HarnessDataEvent::Text(String::from("initial ")),
                    HarnessDataEvent::ToolProposed {
                        continuation_id: String::from("continue-1"),
                        call_id: String::from("call-1"),
                        tool: String::from("filesystem.read"),
                        arguments: json!({"path":"README.md"}),
                    },
                ])),
                Ok(HarnessDataReply::Events(vec![
                    HarnessDataEvent::Started,
                    HarnessDataEvent::Text(String::from("final")),
                    HarnessDataEvent::Completed {
                        reason: String::from("stop"),
                        input_tokens: 2,
                        output_tokens: 2,
                    },
                ])),
            ],
            Ok(vec![
                ToolDataEvent::Started {
                    call_id: String::from("call-1"),
                },
                ToolDataEvent::Completed {
                    call_id: String::from("call-1"),
                    result: json!({"content":"fixture"}),
                    artifact: None,
                    truncated: false,
                },
            ]),
        );
        let logic = TurnLogic::new(data.clone(), tool_approval_policy());
        let awaiting = logic
            .run_turn(command())
            .await
            .expect("request tool approval");
        let continuation_id = awaiting
            .awaiting_continuation
            .expect("approval continuation");
        *data.state.continuation.lock().expect("continuation") = Some(ContinuationRecord {
            session_id: session_id().to_string(),
            id: continuation_id.clone(),
            state: ContinuationStateRecord::Pending,
            wake_condition: ContinuationWakeRecord::Manual,
            payload: ContinuationPayloadRecord::ToolApproval(Box::new(ToolApprovalPayloadRecord {
                session_id: session_id().to_string(),
                workspace: String::from("fixture-workspace"),
                call_id: String::from("call-1"),
                tool: String::from("filesystem.read"),
                arguments: json!({"path":"README.md"}),
                cancellation_id: String::from("cancel-1"),
                provider: String::from("deterministic-mock"),
                model: String::from("fixture"),
                options: json!({}),
                style: String::from("ephemeral-turn"),
                harness_continuation: String::from("continue-1"),
                remaining_tool_calls: Vec::new(),
            })),
            expires_at_millis: None,
        });

        logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id,
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect("resume approved ephemeral tool");

        let state = load_mock_state(&data);
        let assistants = state
            .conversation
            .history()
            .iter()
            .filter_map(|entry| match entry {
                ConversationEntry::AssistantMessage(assistant) => Some(assistant.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(assistants, ["initial final"]);
        assert!(state.conversation.provider_projection().is_empty());
        assert_eq!(
            data.state.harness_commands.lock().expect("commands").len(),
            2
        );

        let full = data.state.events.lock().expect("events").clone();
        let done_cut = full
            .iter()
            .position(|event| {
                event.metadata.event_type == "style.node_entered"
                    && event.payload.to_string().contains("\"done\"")
            })
            .expect("complete-turn entry");
        let recovery = MockTurnData::with_events(
            Err(HarnessDataError::Unavailable),
            full[..=done_cut].to_vec(),
        );
        TurnLogic::new(recovery.clone(), tool_approval_policy())
            .run_turn(command())
            .await
            .expect("recover approval-resumed complete turn");
        assert!(
            recovery
                .state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty(),
            "approval recovery must not redispatch provider or tool work"
        );
        let recovered = load_mock_state(&recovery);
        let recovered_assistants = recovered
            .conversation
            .history()
            .iter()
            .filter_map(|entry| match entry {
                ConversationEntry::AssistantMessage(assistant) => Some(assistant.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(recovered_assistants, ["initial final"]);
        assert!(recovered.conversation.provider_projection().is_empty());
    }

    #[tokio::test]
    async fn started_context_phase_without_completion_fails_closed() {
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(Vec::new())),
            context_recovery_events(false),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic
            .run_turn(command())
            .await
            .expect_err("ambiguous phase");

        assert!(matches!(
            error,
            RunTurnError::AmbiguousContextPhase(ref phase) if phase == "memory"
        ));
        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
        assert_eq!(
            data.event_types()
                .iter()
                .filter(|event| event.as_str() == "context.phase_started")
                .count(),
            2,
            "recovery must retain only the completed turn-start phase and one ambiguous model phase"
        );
    }

    #[tokio::test]
    async fn context_retry_rejects_provider_model_and_options_mismatch_without_mutation() {
        let variants = [
            {
                let mut value = command();
                value.provider = String::from("different-provider");
                value
            },
            {
                let mut value = command();
                value.model = String::from("different-model");
                value
            },
            {
                let mut value = command();
                value.options = json!({"temperature":0.5});
                value
            },
        ];
        for mismatched in variants {
            let data = MockTurnData::with_events(
                Ok(HarnessDataReply::Events(Vec::new())),
                context_recovery_events(true),
            );
            let before = data.event_types();
            let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

            let error = logic
                .run_turn(mismatched)
                .await
                .expect_err("mismatched retry identity");

            assert!(matches!(error, RunTurnError::StyleRecoveryRequired(_)));
            assert_eq!(data.event_types(), before);
            assert!(
                data.state
                    .harness_commands
                    .lock()
                    .expect("commands")
                    .is_empty()
            );
        }
    }

    #[test]
    fn provider_terminal_failures_are_classified_as_failed_style_nodes() {
        assert_eq!(
            provider_node_failure(&[ProviderEvent::Cancelled]),
            Some("model_request_cancelled")
        );
        assert_eq!(
            provider_node_failure(&[ProviderEvent::Failed {
                code: String::from("fixture"),
                message: String::from("fixture failure"),
                retryable: true,
            }]),
            Some("model_request_failed")
        );
        assert_eq!(
            provider_node_failure(&[
                ProviderEvent::Started,
                ProviderEvent::Completed {
                    reason: String::from("stop"),
                    input_tokens: 1,
                    output_tokens: 1,
                },
            ]),
            None
        );
    }

    #[tokio::test]
    async fn completed_node_gap_recovers_transition_then_fails_closed_at_effect_destination() {
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(Vec::new())),
            bound_control_events(false, 3),
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic
            .run_turn(command())
            .await
            .expect_err("recovery required");

        assert!(matches!(
            error,
            RunTurnError::StyleControlRecoveryRequired {
                ref node,
                phase: "awaiting_destination_entry"
            } if node == "tool"
        ));
        assert_eq!(
            data.event_types(),
            vec![
                "session.created",
                "style.execution_initialized",
                "style.node_entered",
                "style.node_completed",
                "style.transition_selected",
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
    async fn selected_transition_gap_fails_closed_without_mutating_or_dispatching() {
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(Vec::new())),
            bound_control_events(true, 3),
        );
        let before = data.event_types();
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic
            .run_turn(command())
            .await
            .expect_err("recovery required");

        assert!(matches!(
            error,
            RunTurnError::StyleControlRecoveryRequired {
                ref node,
                phase: "awaiting_destination_entry"
            } if node == "tool"
        ));
        assert_eq!(data.event_types(), before);
        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn declarative_bound_graph_uses_generic_adapter_without_harness_calls() {
        let binding = binding(BuiltInStyle::DeclarativeGraph);
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(Vec::new())),
            vec![data_event(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture-workspace"),
                    style: String::from("declarative-graph"),
                    style_binding: Some(Box::new(binding)),
                }),
            )],
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let result = logic
            .run_turn(command())
            .await
            .expect("declarative adapter");

        assert!(result.awaiting_continuation.is_none());
        assert_eq!(
            load_mock_state(&data).lifecycle,
            crate::session::SessionLifecycle::Completed
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
    async fn style_step_budget_succeeds_at_exact_limit() {
        let binding = persistent_binding(3);
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(vec![
                HarnessDataEvent::Started,
                HarnessDataEvent::Text(String::from("done")),
                HarnessDataEvent::Completed {
                    reason: String::from("stop"),
                    input_tokens: 1,
                    output_tokens: 1,
                },
            ])),
            vec![data_event(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture-workspace"),
                    style: String::from("persistent-chat"),
                    style_binding: Some(Box::new(binding)),
                }),
            )],
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        logic.run_turn(command()).await.expect("exact-limit turn");

        let types = data.event_types();
        assert_eq!(
            types
                .iter()
                .filter(|event_type| event_type.as_str() == "style.node_entered")
                .count(),
            3
        );
        assert!(
            !types
                .iter()
                .any(|event_type| event_type == "style.execution_terminated")
        );
    }

    #[tokio::test]
    async fn style_step_budget_terminates_before_first_over_limit_entry() {
        let binding = persistent_binding(1);
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(vec![
                HarnessDataEvent::Started,
                HarnessDataEvent::Completed {
                    reason: String::from("stop"),
                    input_tokens: 1,
                    output_tokens: 1,
                },
            ])),
            vec![data_event(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture-workspace"),
                    style: String::from("persistent-chat"),
                    style_binding: Some(Box::new(binding)),
                }),
            )],
        );
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic.run_turn(command()).await.expect_err("step limit");

        assert!(matches!(
            error,
            RunTurnError::StyleStepBudgetExceeded { limit: 1 }
        ));
        let types = data.event_types();
        assert_eq!(
            types
                .iter()
                .filter(|event_type| event_type.as_str() == "style.node_entered")
                .count(),
            1
        );
        assert_eq!(
            types.last().map(String::as_str),
            Some("style.execution_terminated")
        );
    }

    #[tokio::test]
    async fn post_tool_provider_cancellation_and_failure_fail_the_active_style_node() {
        for terminal in [
            HarnessDataEvent::Cancelled,
            HarnessDataEvent::Failed {
                code: String::from("fixture"),
                message: String::from("continuation failed"),
                retryable: false,
            },
        ] {
            let binding = persistent_binding(3);
            let data = MockTurnData::with_scenario(
                vec![data_event(
                    1,
                    RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                        workspace: String::from("fixture-workspace"),
                        style: String::from("persistent-chat"),
                        style_binding: Some(Box::new(binding)),
                    }),
                )],
                vec![
                    Ok(HarnessDataReply::Events(vec![
                        HarnessDataEvent::Started,
                        HarnessDataEvent::ToolProposed {
                            continuation_id: String::from("continue-1"),
                            call_id: String::from("call-1"),
                            tool: String::from("filesystem.read"),
                            arguments: json!({"path":"README.md"}),
                        },
                    ])),
                    Ok(HarnessDataReply::Events(vec![
                        HarnessDataEvent::Started,
                        terminal,
                    ])),
                ],
                Ok(vec![
                    ToolDataEvent::Started {
                        call_id: String::from("call-1"),
                    },
                    ToolDataEvent::Completed {
                        call_id: String::from("call-1"),
                        result: json!({"content":"fixture"}),
                        artifact: None,
                        truncated: false,
                    },
                ]),
            );
            let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

            let result = logic.run_turn(command()).await.expect("classified result");

            assert_eq!(
                data.event_types().last().map(String::as_str),
                Some("style.node_failed")
            );
            assert_eq!(
                data.event_types()
                    .iter()
                    .filter(|event_type| event_type.as_str() == "style.node_completed")
                    .count(),
                1,
                "tool and terminal graph nodes must not complete after provider failure"
            );
            assert_eq!(
                result.last_committed_sequence.get(),
                u64::try_from(data.event_types().len()).expect("event count")
            );
            let loaded = SessionPersistenceLogic::new(data.clone())
                .load_session(LoadSessionCommand {
                    session_directory: PathBuf::from("sessions").join(session_id().to_string()),
                    expected_session_id: session_id(),
                })
                .expect("replay");
            let execution = loaded.state.style_execution.expect("style execution");
            assert!(execution.active_node.is_none());
            assert!(matches!(
                execution.control,
                StyleExecutionControlState::ReadyForEntry(_)
            ));
        }
    }

    #[test]
    fn cancellation_after_approval_reload_fails_tool_node_without_completing_graph() {
        let continuation_id = ContinuationId::from_uuid(Uuid::from_u128(99));
        let mut events = bound_control_events(true, 3);
        events.extend([
            data_event(
                6,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("tool"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 2,
                }),
            ),
            data_event(
                7,
                RuntimeCommittedEvent::ApprovalRequested(ApprovalRequestedEvent {
                    continuation_id,
                    action_summary: String::from("fixture approval"),
                }),
            ),
            data_event(
                8,
                RuntimeCommittedEvent::ApprovalResolved(ApprovalResolvedEvent {
                    continuation_id,
                    approved: true,
                }),
            ),
            data_event(
                9,
                RuntimeCommittedEvent::ModelRequestCancelled(ModelRequestCancelledEvent {
                    cancellation_id: String::from("cancel-1"),
                }),
            ),
        ]);
        let data = MockTurnData::with_events(Ok(HarnessDataReply::Events(Vec::new())), events);
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));
        let persistence = SessionPersistenceLogic::new(data.clone());

        let position = logic
            .fail_active_bound_style_at_head(
                &persistence,
                session_id(),
                &PathBuf::from("sessions").join(session_id().to_string()),
                "model_request_cancelled",
            )
            .expect("classification")
            .expect("bound style");

        assert_eq!(position.sequence.get(), 10);
        assert_eq!(
            data.event_types().last().map(String::as_str),
            Some("style.node_failed")
        );
        let loaded = persistence
            .load_session(LoadSessionCommand {
                session_directory: PathBuf::from("sessions").join(session_id().to_string()),
                expected_session_id: session_id(),
            })
            .expect("replay");
        let execution = loaded.state.style_execution.expect("style execution");
        assert!(execution.active_node.is_none());
        assert_eq!(execution.completed_nodes.len(), 1);
        assert!(matches!(
            execution.control,
            StyleExecutionControlState::ReadyForEntry(_)
        ));
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
    fn terminal_approved_tool_still_has_one_pending_model_resume() {
        let mut events = bound_control_events(true, 3);
        let digest = terminal_tool_action_digest();
        events.extend([
            data_event(
                6,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("tool"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 2,
                }),
            ),
            data_event(
                7,
                RuntimeCommittedEvent::ToolExecutionDispatched(ToolExecutionDispatchedEvent {
                    execution_id: String::from("execution-1"),
                    call_id: String::from("call-1"),
                    action_digest: digest,
                }),
            ),
            data_event(
                8,
                RuntimeCommittedEvent::ToolExecutionStarted(ToolExecutionStartedEvent {
                    call_id: String::from("call-1"),
                }),
            ),
            data_event(
                9,
                RuntimeCommittedEvent::ToolExecutionCompleted(ToolExecutionCompletedEvent {
                    call_id: String::from("call-1"),
                    result: json!({"ok":true}),
                    artifact: None,
                    truncated: false,
                }),
            ),
        ]);
        let data = MockTurnData::with_events(Ok(HarnessDataReply::Events(Vec::new())), events);
        let state = SessionPersistenceLogic::new(data)
            .load_session(LoadSessionCommand {
                session_directory: PathBuf::from("sessions").join(session_id().to_string()),
                expected_session_id: session_id(),
            })
            .expect("replay")
            .state;
        assert!(
            pending_model_resume_after_terminal_tool(&state, state.tool_executions.get("call-1"))
                .expect("safe resume")
        );
    }

    fn terminal_tool_action_digest() -> ContentHash {
        ActionProposal {
            id: ProposalId(String::from("tool-call:call-1")),
            action: ConsequentialAction::ToolCall(crate::action::ToolCallAction {
                tool: String::from("filesystem.read"),
                group: String::from("filesystem"),
                arguments: json!({"path":"README.md"}),
                source: None,
            }),
            style: String::from("persistent-chat"),
            workspace: String::from("fixture-workspace"),
            origin: String::from("runtime"),
        }
        .digest()
        .expect("terminal tool action digest")
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture exposes the exact approval, receipt, and conversation crash positions"
    )]
    fn terminal_approval_fixture(
        approved: bool,
        conversation_entries: Vec<ConversationEntry>,
        approved_digest: Option<ContentHash>,
    ) -> (MockTurnData, ContinuationId) {
        let continuation_id = ContinuationId::from_uuid(Uuid::from_u128(99));
        let mut events = bound_control_events(true, 3);
        events.extend([
            data_event(
                6,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent {
                        entry: ConversationEntry::UserMessage(text_entry(
                            "user:6:cancel-1",
                            "inspect the repository",
                            6,
                        )),
                    },
                ),
            ),
            data_event(
                7,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("tool"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 2,
                }),
            ),
            data_event(
                8,
                RuntimeCommittedEvent::ApprovalRequested(ApprovalRequestedEvent {
                    continuation_id,
                    action_summary: String::from("fixture approval"),
                }),
            ),
            data_event(
                9,
                RuntimeCommittedEvent::ApprovalResolved(ApprovalResolvedEvent {
                    continuation_id,
                    approved,
                }),
            ),
        ]);
        if approved {
            let digest = approved_digest.unwrap_or_else(terminal_tool_action_digest);
            events.extend([
                data_event(
                    10,
                    RuntimeCommittedEvent::ToolExecutionDispatched(ToolExecutionDispatchedEvent {
                        execution_id: String::from("execution-1"),
                        call_id: String::from("call-1"),
                        action_digest: digest,
                    }),
                ),
                data_event(
                    11,
                    RuntimeCommittedEvent::ToolExecutionStarted(ToolExecutionStartedEvent {
                        call_id: String::from("call-1"),
                    }),
                ),
                data_event(
                    12,
                    RuntimeCommittedEvent::ToolExecutionCompleted(ToolExecutionCompletedEvent {
                        call_id: String::from("call-1"),
                        result: json!({"content":"fixture"}),
                        artifact: None,
                        truncated: false,
                    }),
                ),
            ]);
        } else {
            events.push(data_event(
                10,
                RuntimeCommittedEvent::ToolExecutionFailed(ToolExecutionFailedEvent {
                    call_id: String::from("call-1"),
                    action_digest: Some(terminal_tool_action_digest()),
                    code: String::from("permission_denied"),
                    message: String::from("user denied the requested action"),
                    retryable: false,
                }),
            ));
        }
        for entry in conversation_entries {
            let sequence = u64::try_from(events.len() + 1).expect("sequence");
            events.push(data_event(
                sequence,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent { entry },
                ),
            ));
        }
        let continuation = ContinuationRecord {
            session_id: session_id().to_string(),
            id: continuation_id.to_string(),
            state: if approved {
                ContinuationStateRecord::Resumed
            } else {
                ContinuationStateRecord::Cancelled
            },
            wake_condition: ContinuationWakeRecord::Manual,
            payload: ContinuationPayloadRecord::ToolApproval(Box::new(ToolApprovalPayloadRecord {
                session_id: session_id().to_string(),
                workspace: String::from("fixture-workspace"),
                call_id: String::from("call-1"),
                tool: String::from("filesystem.read"),
                arguments: json!({"path":"README.md"}),
                cancellation_id: String::from("cancel-1"),
                provider: String::from("deterministic-mock"),
                model: String::from("fixture"),
                options: json!({}),
                style: String::from("persistent-chat"),
                harness_continuation: String::from("continue-1"),
                remaining_tool_calls: Vec::new(),
            })),
            expires_at_millis: None,
        };
        (
            MockTurnData::with_events(
                Ok(HarnessDataReply::Events(vec![
                    HarnessDataEvent::Started,
                    HarnessDataEvent::Text(String::from("done")),
                    HarnessDataEvent::Completed {
                        reason: String::from("stop"),
                        input_tokens: 4,
                        output_tokens: 1,
                    },
                ])),
                events,
            )
            .with_continuation(continuation),
            continuation_id,
        )
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the full recovery fixture intentionally exposes every canonical lifecycle event"
    )]
    async fn repeated_approved_resolution_resumes_terminal_tool_without_duplicate_effects() {
        let continuation_id = ContinuationId::from_uuid(Uuid::from_u128(99));
        let mut events = bound_control_events(true, 3);
        let digest = terminal_tool_action_digest();
        events.extend([
            data_event(
                6,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent {
                        entry: ConversationEntry::UserMessage(text_entry(
                            "user:6:cancel-1",
                            "inspect the repository",
                            6,
                        )),
                    },
                ),
            ),
            data_event(
                7,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("tool"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 2,
                }),
            ),
            data_event(
                8,
                RuntimeCommittedEvent::ApprovalRequested(ApprovalRequestedEvent {
                    continuation_id,
                    action_summary: String::from("fixture approval"),
                }),
            ),
            data_event(
                9,
                RuntimeCommittedEvent::ApprovalResolved(ApprovalResolvedEvent {
                    continuation_id,
                    approved: true,
                }),
            ),
            data_event(
                10,
                RuntimeCommittedEvent::ToolExecutionDispatched(ToolExecutionDispatchedEvent {
                    execution_id: String::from("execution-1"),
                    call_id: String::from("call-1"),
                    action_digest: digest,
                }),
            ),
            data_event(
                11,
                RuntimeCommittedEvent::ToolExecutionStarted(ToolExecutionStartedEvent {
                    call_id: String::from("call-1"),
                }),
            ),
            data_event(
                12,
                RuntimeCommittedEvent::ToolExecutionCompleted(ToolExecutionCompletedEvent {
                    call_id: String::from("call-1"),
                    result: json!({"content":"fixture"}),
                    artifact: None,
                    truncated: false,
                }),
            ),
        ]);
        let continuation = ContinuationRecord {
            session_id: session_id().to_string(),
            id: continuation_id.to_string(),
            state: ContinuationStateRecord::Resumed,
            wake_condition: ContinuationWakeRecord::Manual,
            payload: ContinuationPayloadRecord::ToolApproval(Box::new(ToolApprovalPayloadRecord {
                session_id: session_id().to_string(),
                workspace: String::from("fixture-workspace"),
                call_id: String::from("call-1"),
                tool: String::from("filesystem.read"),
                arguments: json!({"path":"README.md"}),
                cancellation_id: String::from("cancel-1"),
                provider: String::from("deterministic-mock"),
                model: String::from("fixture"),
                options: json!({}),
                style: String::from("persistent-chat"),
                harness_continuation: String::from("continue-1"),
                remaining_tool_calls: Vec::new(),
            })),
            expires_at_millis: None,
        };
        let data = MockTurnData::with_events(
            Ok(HarnessDataReply::Events(vec![
                HarnessDataEvent::Started,
                HarnessDataEvent::Text(String::from("done")),
                HarnessDataEvent::Completed {
                    reason: String::from("stop"),
                    input_tokens: 4,
                    output_tokens: 1,
                },
            ])),
            events,
        )
        .with_continuation(continuation);
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let result = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.to_string(),
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect("resume terminal approved tool");

        assert!(result.transitioned);
        assert_eq!(result.awaiting_continuation, None);
        let event_types = data.event_types();
        assert_eq!(
            event_types
                .iter()
                .filter(|event_type| event_type.as_str() == "tool.execution_completed")
                .count(),
            1,
            "the terminal tool receipt must not be duplicated"
        );
        assert!(
            !event_types
                .iter()
                .any(|event_type| event_type == "tool.execution_failed"),
            "an approved terminal tool must not be rewritten as a failure"
        );
        assert_eq!(
            data.state.harness_commands.lock().expect("commands").len(),
            1,
            "exactly one pending provider resume is allowed"
        );
        assert!(
            !data
                .state
                .events
                .lock()
                .expect("events")
                .iter()
                .any(|event| event.payload.to_string().contains("permission_denied")),
            "an approved terminal tool must not append denial state"
        );
        let state = SessionPersistenceLogic::new(data.clone())
            .load_session(LoadSessionCommand {
                session_directory: PathBuf::from("sessions").join(session_id().to_string()),
                expected_session_id: session_id(),
            })
            .expect("replay")
            .state;
        assert_eq!(
            state
                .conversation
                .history()
                .iter()
                .filter(|entry| matches!(
                    entry,
                    ConversationEntry::ToolCallRequest(call) if call.call_id == "call-1"
                ))
                .count(),
            1
        );
        assert_eq!(
            state
                .conversation
                .history()
                .iter()
                .filter(|entry| matches!(
                    entry,
                    ConversationEntry::ToolResult(result) if result.call_id == "call-1"
                ))
                .count(),
            1
        );
        assert_eq!(
            event_types.last().map(String::as_str),
            Some("style.node_completed")
        );
    }

    #[tokio::test]
    async fn terminal_approval_repairs_exact_partial_call_without_redispatch() {
        let call = ConversationEntry::ToolCallRequest(ToolCallEntry {
            id: ConversationEntryId(String::from("tool-call:call-1:13")),
            call_id: String::from("call-1"),
            tool: String::from("filesystem.read"),
            arguments: json!({"path":"README.md"}),
            source_sequence: Sequence::new(13).expect("sequence"),
        });
        let (data, continuation_id) = terminal_approval_fixture(true, vec![call], None);
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.to_string(),
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect("repair partial tool conversation");

        let state = SessionPersistenceLogic::new(data.clone())
            .load_session(LoadSessionCommand {
                session_directory: PathBuf::from("sessions").join(session_id().to_string()),
                expected_session_id: session_id(),
            })
            .expect("replay")
            .state;
        assert_eq!(
            state
                .conversation
                .history()
                .iter()
                .filter(|entry| matches!(
                    entry,
                    ConversationEntry::ToolCallRequest(call) if call.call_id == "call-1"
                ))
                .count(),
            1
        );
        assert_eq!(
            state
                .conversation
                .history()
                .iter()
                .filter(|entry| matches!(
                    entry,
                    ConversationEntry::ToolResult(result) if result.call_id == "call-1"
                ))
                .count(),
            1
        );
        assert_eq!(
            data.event_types()
                .iter()
                .filter(|kind| kind.as_str() == "tool.execution_completed")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn terminal_approval_rejects_conflicting_partial_call_without_mutation() {
        let call = ConversationEntry::ToolCallRequest(ToolCallEntry {
            id: ConversationEntryId(String::from("tool-call:call-1:13")),
            call_id: String::from("call-1"),
            tool: String::from("filesystem.read"),
            arguments: json!({"path":"DIFFERENT.md"}),
            source_sequence: Sequence::new(13).expect("sequence"),
        });
        let (data, continuation_id) = terminal_approval_fixture(true, vec![call], None);
        let before = data.event_types();
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.to_string(),
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect_err("conflicting partial call");

        assert!(matches!(
            error,
            RunTurnError::ToolConversationRecoveryConflict(ref call) if call == "call-1"
        ));
        assert_eq!(data.event_types(), before);
        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn terminal_approval_rejects_mismatched_receipt_digest_without_mutation() {
        let (data, continuation_id) = terminal_approval_fixture(
            true,
            Vec::new(),
            Some(ContentHash::digest(b"different approved action")),
        );
        let before = data.event_types();
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.to_string(),
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect_err("mismatched terminal receipt");

        assert!(matches!(
            error,
            RunTurnError::InvalidRecoveryReceipt(ref call) if call == "call-1"
        ));
        assert_eq!(data.event_types(), before);
        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn terminal_approval_rejects_reversed_conversation_pair_without_mutation() {
        let result = ConversationEntry::ToolResult(ToolResultEntry {
            id: ConversationEntryId(String::from("tool-result:call-1:13")),
            call_id: String::from("call-1"),
            content: String::from(r#"{"content":"fixture"}"#),
            artifact_id: None,
            truncated: false,
            source_sequence: Sequence::new(13).expect("sequence"),
        });
        let call = ConversationEntry::ToolCallRequest(ToolCallEntry {
            id: ConversationEntryId(String::from("tool-call:call-1:14")),
            call_id: String::from("call-1"),
            tool: String::from("filesystem.read"),
            arguments: json!({"path":"README.md"}),
            source_sequence: Sequence::new(14).expect("sequence"),
        });
        let (data, continuation_id) = terminal_approval_fixture(true, vec![result, call], None);
        let before = data.event_types();
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        let error = logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.to_string(),
                approved: true,
                resume_after_resolution: true,
            })
            .await
            .expect_err("reversed tool conversation");

        assert!(matches!(
            error,
            RunTurnError::ToolConversationRecoveryConflict(ref call) if call == "call-1"
        ));
        assert_eq!(data.event_types(), before);
        assert!(
            data.state
                .harness_commands
                .lock()
                .expect("commands")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn denied_terminal_gap_repairs_structured_failure_without_dispatch() {
        let (data, continuation_id) = terminal_approval_fixture(false, Vec::new(), None);
        let logic = TurnLogic::new(data.clone(), policy(PermissionEffect::Allow));

        logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id: session_id().to_string(),
                continuation_id: continuation_id.to_string(),
                approved: false,
                resume_after_resolution: true,
            })
            .await
            .expect("repair denied terminal gap");

        assert_eq!(
            data.event_types()
                .iter()
                .filter(|kind| kind.as_str() == "tool.execution_failed")
                .count(),
            1
        );
        assert!(
            data.state
                .events
                .lock()
                .expect("events")
                .iter()
                .any(|event| event.payload.to_string().contains("permission_denied"))
        );
        assert_eq!(
            data.state.harness_commands.lock().expect("commands").len(),
            1
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

    #[test]
    fn planner_phase_cancellation_ids_are_typed_deterministic_and_distinct() {
        let base = Uuid::from_u128(42).hyphenated().to_string();
        let plan = planner_phase_cancellation_id(&base, "plan", 0);
        let repeated_plan = planner_phase_cancellation_id(&base, "plan", 0);
        let integration = planner_phase_cancellation_id(&base, "integrate", 0);
        let next_review = planner_phase_cancellation_id(&base, "review", 1);

        assert!(Uuid::parse_str(&plan).is_ok());
        assert_eq!(plan, repeated_plan);
        assert_ne!(plan, integration);
        assert_ne!(integration, next_review);
        assert_ne!(plan, base);
    }

    #[test]
    fn planner_child_cancellation_ids_are_typed_and_task_scoped() {
        let base = Uuid::from_u128(42).hyphenated().to_string();
        let first = planner_child_cancellation_id(&base, "task-1", 0);
        let repeated = planner_child_cancellation_id(&base, "task-1", 0);
        let second = planner_child_cancellation_id(&base, "task-2", 0);
        let revision = planner_child_cancellation_id(&base, "task-1", 1);

        assert!(Uuid::parse_str(&first).is_ok());
        assert_eq!(first, repeated);
        assert_ne!(first, second);
        assert_ne!(first, revision);
    }

    #[test]
    fn planner_task_output_is_bounded_and_rejects_duplicate_ids() {
        let valid = vec![ProviderEvent::Text(String::from(
            r#"{"tasks":[{"task_id":"one","description":"first"},{"task_id":"two","description":"second"}]}"#,
        ))];
        let tasks = parse_planner_tasks(&valid, 2).expect("valid task plan");
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["one", "two"]
        );

        let duplicate = vec![ProviderEvent::Text(String::from(
            r#"{"tasks":[{"task_id":"one","description":"first"},{"task_id":"one","description":"second"}]}"#,
        ))];
        assert!(matches!(
            parse_planner_tasks(&duplicate, 2),
            Err(RunTurnError::PlannerOutputInvalid)
        ));
        assert!(matches!(
            parse_planner_tasks(&valid, 1),
            Err(RunTurnError::PlannerOutputInvalid)
        ));
    }

    #[test]
    fn reviewer_output_requires_consistent_known_rejections() {
        let tasks = BTreeMap::from([
            (
                String::from("one"),
                PlannedTask {
                    task_id: String::from("one"),
                    description: String::from("first"),
                },
            ),
            (
                String::from("two"),
                PlannedTask {
                    task_id: String::from("two"),
                    description: String::from("second"),
                },
            ),
        ]);
        let rejected = vec![ProviderEvent::Text(String::from(
            r#"{"approved":false,"rejected_task_ids":["two"],"findings":["revise two"]}"#,
        ))];
        assert_eq!(
            parse_reviewer_findings(&rejected, &tasks).expect("valid rejection"),
            (
                false,
                vec![String::from("two")],
                vec![String::from("revise two")]
            )
        );

        for invalid in [
            r#"{"approved":true,"rejected_task_ids":["two"],"findings":["conflict"]}"#,
            r#"{"approved":false,"rejected_task_ids":["missing"],"findings":["unknown"]}"#,
            r#"{"approved":false,"rejected_task_ids":["two","two"],"findings":["duplicate"]}"#,
        ] {
            assert!(matches!(
                parse_reviewer_findings(&[ProviderEvent::Text(String::from(invalid))], &tasks),
                Err(RunTurnError::ReviewerOutputInvalid)
            ));
        }
    }
}
