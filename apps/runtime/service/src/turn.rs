//! Durable turn endpoint mapping and composite runtime routing.
#![allow(
    missing_docs,
    reason = "service-local turn records are intentionally boundary-specific"
)]

use std::{path::PathBuf, sync::Arc};

use agentmod_runtime_logic::{
    continuation::{ContinuationWakeCondition, ContinuationWakeProof},
    harness::ProviderEvent,
    plugin_management::{PluginAuditRecord, PluginLifecycleProjection, PluginManagementLogicPort},
    turn::{
        ApprovalTurnLogicPort, CancelTurnCommand, CancelTurnLogicPort,
        CommittedEventObserverLogicPort, CreateDeferredTurnCommand, DeferredTurnLogicPort,
        ObserveCommittedEventsCommand, RecordScheduledRecoveryCommand, RecoverStartupToolsCommand,
        ResolveTurnApprovalCommand, RunScheduledTurnCommand, RunTurnCommand, RunTurnError,
        RunTurnStream, RunTurnStreamItem, ScheduledRecoveryLogicPort, StartupToolRecoveryLogicPort,
        TurnLogicPort, WakeScheduledTurnCommand,
    },
};
use agentmod_runtime_protocol::{
    RuntimeProviderEvent, RuntimeRequest, RuntimeResponse, RuntimeScheduleTrigger,
    RuntimeScheduledRun,
};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{RuntimeService, ServiceSchedulePayload, ServiceScheduledExecution};

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceRunTurnRequest {
    pub session_id: String,
    pub prompt: String,
    pub provider: String,
    pub model: String,
    pub options: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceRunTurnResponse {
    pub events: Vec<ServiceTurnEvent>,
    pub first_committed_sequence: agentmod_primitives::Sequence,
    pub last_committed_sequence: agentmod_primitives::Sequence,
    pub awaiting_continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceRunScheduledTurnRequest {
    pub execution_id: String,
    pub schedule_id: String,
    pub scheduled_for_ms: i64,
    pub turn: ServiceRunTurnRequest,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceCreateDeferredTurnRequest {
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
    pub wake_condition: ServiceContinuationWakeCondition,
    pub expires_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceContinuationWakeCondition {
    AtMillis(i64),
    RuntimeEvent {
        event_type: String,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceContinuationWakeProof {
    AtMillis(i64),
    RuntimeEvent {
        event_type: String,
        observed_at_ms: i64,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
        observed_at_ms: i64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceWakeScheduledTurnRequest {
    pub session_id: String,
    pub continuation_id: String,
    pub execution_id: String,
    pub schedule_id: String,
    pub scheduled_for_ms: i64,
    pub proof: ServiceContinuationWakeProof,
    pub allow_resumed_recovery: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceWakeScheduledTurnResponse {
    pub transitioned: bool,
    pub turn: Option<ServiceRunTurnResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRecordScheduledRecoveryRequest {
    pub session_id: String,
    pub execution_id: String,
    pub schedule_id: String,
    pub outcome: String,
    pub continuation_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceStartupRecoveryResult {
    pub receipt_count: usize,
    pub reconciled_count: usize,
    pub already_terminal_count: usize,
    pub deferred_approval_count: usize,
    pub orphaned_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ServiceTurnEvent {
    Started,
    Text(String),
    ToolDelta {
        call_id: String,
        name: String,
        arguments: String,
    },
    ToolProposed {
        continuation_id: String,
        call_id: String,
        tool: String,
        arguments: Value,
    },
    Completed {
        reason: String,
        input_tokens: u64,
        output_tokens: u64,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

pub enum ServiceTurnStreamItem {
    Event {
        event: ServiceTurnEvent,
        committed_sequence: agentmod_primitives::Sequence,
    },
    Complete {
        first_committed_sequence: agentmod_primitives::Sequence,
        last_committed_sequence: agentmod_primitives::Sequence,
        awaiting_continuation: Option<String>,
    },
}

pub struct ServiceTurnStream {
    logic: RunTurnStream,
}

impl ServiceTurnStream {
    pub async fn next(&mut self) -> Option<Result<ServiceTurnStreamItem, TurnServiceError>> {
        self.logic.next().await.map(|result| {
            result
                .map(|item| match item {
                    RunTurnStreamItem::Event {
                        event,
                        committed_sequence,
                    } => ServiceTurnStreamItem::Event {
                        event: map_event(event),
                        committed_sequence,
                    },
                    RunTurnStreamItem::Complete {
                        first_committed_sequence,
                        last_committed_sequence,
                        awaiting_continuation,
                    } => ServiceTurnStreamItem::Complete {
                        first_committed_sequence,
                        last_committed_sequence,
                        awaiting_continuation,
                    },
                })
                .map_err(TurnServiceError::Logic)
        })
    }
}

#[derive(Clone)]
pub struct TurnService<L> {
    logic: L,
    sessions_root: PathBuf,
}

impl<L> TurnService<L> {
    #[must_use]
    pub const fn new(logic: L, sessions_root: PathBuf) -> Self {
        Self {
            logic,
            sessions_root,
        }
    }
}

impl<L: TurnLogicPort> TurnService<L> {
    /// Maps and executes one service-owned durable turn request.
    ///
    /// # Errors
    ///
    /// Returns [`TurnServiceError`] for endpoint validation or translated
    /// runtime business failures.
    pub async fn run_turn(
        &self,
        request: ServiceRunTurnRequest,
    ) -> Result<ServiceRunTurnResponse, TurnServiceError> {
        if request.prompt.trim().is_empty() {
            return Err(TurnServiceError::Invalid);
        }
        let result = self
            .logic
            .run_turn(RunTurnCommand {
                sessions_root: self.sessions_root.clone(),
                session_id: request.session_id,
                prompt: request.prompt,
                provider: request.provider,
                model: request.model,
                options: request.options,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(TurnServiceError::Logic)?;
        Ok(ServiceRunTurnResponse {
            events: result.events.into_iter().map(map_event).collect(),
            first_committed_sequence: result.first_committed_sequence,
            last_committed_sequence: result.last_committed_sequence,
            awaiting_continuation: result.awaiting_continuation,
        })
    }

    /// Commits scheduler provenance and executes a normal intercepted turn.
    ///
    /// # Errors
    ///
    /// Returns [`TurnServiceError`] for invalid endpoint input or business failure.
    pub async fn run_scheduled_turn(
        &self,
        request: ServiceRunScheduledTurnRequest,
    ) -> Result<ServiceRunTurnResponse, TurnServiceError> {
        if request.turn.prompt.trim().is_empty() {
            return Err(TurnServiceError::Invalid);
        }
        let result = self
            .logic
            .run_scheduled_turn(RunScheduledTurnCommand {
                execution_id: request.execution_id,
                schedule_id: request.schedule_id,
                scheduled_for_ms: request.scheduled_for_ms,
                turn: RunTurnCommand {
                    sessions_root: self.sessions_root.clone(),
                    session_id: request.turn.session_id,
                    prompt: request.turn.prompt,
                    provider: request.turn.provider,
                    model: request.turn.model,
                    options: request.turn.options,
                    cancellation_id: request.turn.cancellation_id,
                },
            })
            .await
            .map_err(TurnServiceError::Logic)?;
        Ok(ServiceRunTurnResponse {
            events: result.events.into_iter().map(map_event).collect(),
            first_committed_sequence: result.first_committed_sequence,
            last_committed_sequence: result.last_committed_sequence,
            awaiting_continuation: result.awaiting_continuation,
        })
    }

    /// Starts a bounded stream whose events have already been committed.
    ///
    /// # Errors
    ///
    /// Returns [`TurnServiceError`] for invalid endpoint input or business failure.
    pub async fn run_turn_stream(
        &self,
        request: ServiceRunTurnRequest,
    ) -> Result<ServiceTurnStream, TurnServiceError> {
        if request.prompt.trim().is_empty() {
            return Err(TurnServiceError::Invalid);
        }
        self.logic
            .run_turn_stream(RunTurnCommand {
                sessions_root: self.sessions_root.clone(),
                session_id: request.session_id,
                prompt: request.prompt,
                provider: request.provider,
                model: request.model,
                options: request.options,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map(|logic| ServiceTurnStream { logic })
            .map_err(TurnServiceError::Logic)
    }
}

impl<L: CommittedEventObserverLogicPort> TurnService<L> {
    async fn observe_committed_events(
        &self,
        session_id: agentmod_primitives::SessionId,
        events: Vec<crate::ServiceSessionEvent>,
    ) -> Result<agentmod_runtime_logic::plugin::PluginObservationSummary, TurnServiceError> {
        self.logic
            .observe_committed_events(ObserveCommittedEventsCommand {
                sessions_root: self.sessions_root.clone(),
                session_id: session_id.to_string(),
                events: events
                    .into_iter()
                    .map(
                        |event| agentmod_runtime_logic::plugin::CommittedPluginEvent {
                            event_id: event.event_id.to_string(),
                            sequence: event.sequence.get(),
                            event_type: event.event_type,
                            payload: event.payload,
                        },
                    )
                    .collect(),
            })
            .await
            .map_err(TurnServiceError::Logic)
    }
}

impl<L: ApprovalTurnLogicPort> TurnService<L> {
    /// Resolves a durable approval and maps resumed lifecycle events.
    ///
    /// # Errors
    ///
    /// Returns [`TurnServiceError`] when runtime business resolution fails.
    pub async fn resolve_approval(
        &self,
        session_id: String,
        continuation_id: String,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<
        (
            bool,
            Vec<ServiceTurnEvent>,
            agentmod_primitives::Sequence,
            Option<String>,
        ),
        TurnServiceError,
    > {
        let result = self
            .logic
            .resolve_turn_approval(ResolveTurnApprovalCommand {
                sessions_root: self.sessions_root.clone(),
                session_id,
                continuation_id,
                approved,
                resume_after_resolution,
            })
            .await
            .map_err(TurnServiceError::Logic)?;
        Ok((
            result.transitioned,
            result.events.into_iter().map(map_event).collect(),
            result.last_committed_sequence,
            result.awaiting_continuation,
        ))
    }
}

impl<L: DeferredTurnLogicPort> TurnService<L> {
    /// Persists a schedule-bound resume-once continuation.
    ///
    /// # Errors
    ///
    /// Returns [`TurnServiceError`] for invalid endpoint input or business failure.
    pub fn create_deferred_turn(
        &self,
        request: ServiceCreateDeferredTurnRequest,
    ) -> Result<(), TurnServiceError> {
        if request.prompt.trim().is_empty()
            || request.schedule_id.trim().is_empty()
            || request.workspace.trim().is_empty()
            || request.provider.trim().is_empty()
            || request.model.trim().is_empty()
            || request.style.trim().is_empty()
        {
            return Err(TurnServiceError::Invalid);
        }
        self.logic
            .create_deferred_turn(CreateDeferredTurnCommand {
                session_id: request.session_id,
                continuation_id: request.continuation_id,
                schedule_id: request.schedule_id,
                prompt: request.prompt,
                workspace: request.workspace,
                provider: request.provider,
                model: request.model,
                options: request.options,
                style: request.style,
                cancellation_id: request.cancellation_id,
                wake_condition: to_logic_wake_condition(request.wake_condition),
                expires_at: request
                    .expires_at_ms
                    .map(agentmod_primitives::TimestampMillis::new),
            })
            .map_err(TurnServiceError::Logic)
    }

    /// Claims and executes a schedule-bound continuation through the normal turn path.
    ///
    /// # Errors
    ///
    /// Returns [`TurnServiceError`] for an invalid proof or business failure.
    pub async fn wake_scheduled_turn(
        &self,
        request: ServiceWakeScheduledTurnRequest,
    ) -> Result<ServiceWakeScheduledTurnResponse, TurnServiceError> {
        let result = self
            .logic
            .wake_scheduled_turn(WakeScheduledTurnCommand {
                sessions_root: self.sessions_root.clone(),
                session_id: request.session_id,
                continuation_id: request.continuation_id,
                execution_id: request.execution_id,
                schedule_id: request.schedule_id,
                scheduled_for_ms: request.scheduled_for_ms,
                proof: to_logic_wake_proof(request.proof),
                allow_resumed_recovery: request.allow_resumed_recovery,
            })
            .await
            .map_err(TurnServiceError::Logic)?;
        Ok(ServiceWakeScheduledTurnResponse {
            transitioned: result.transitioned,
            turn: result.turn.map(|turn| ServiceRunTurnResponse {
                events: turn.events.into_iter().map(map_event).collect(),
                first_committed_sequence: turn.first_committed_sequence,
                last_committed_sequence: turn.last_committed_sequence,
                awaiting_continuation: turn.awaiting_continuation,
            }),
        })
    }
}

impl<L: CancelTurnLogicPort> TurnService<L> {
    /// Cancels one active provider request.
    ///
    /// # Errors
    ///
    /// Returns a translated runtime business failure.
    pub async fn cancel_turn(
        &self,
        cancellation_id: String,
        reason: String,
    ) -> Result<(), TurnServiceError> {
        self.logic
            .cancel_turn(CancelTurnCommand {
                cancellation_id,
                reason,
            })
            .await
            .map_err(TurnServiceError::Logic)
    }
}

impl<L: ScheduledRecoveryLogicPort> TurnService<L> {
    /// Commits one canonical scheduler reconciliation outcome.
    ///
    /// # Errors
    ///
    /// Returns a translated runtime business failure when the recovery
    /// identity or canonical journal cannot be validated.
    pub fn record_scheduled_recovery(
        &self,
        request: ServiceRecordScheduledRecoveryRequest,
    ) -> Result<agentmod_primitives::Sequence, TurnServiceError> {
        self.logic
            .record_scheduled_recovery(RecordScheduledRecoveryCommand {
                sessions_root: self.sessions_root.clone(),
                session_id: request.session_id,
                execution_id: request.execution_id,
                schedule_id: request.schedule_id,
                outcome: request.outcome,
                continuation_id: request.continuation_id,
            })
            .map_err(TurnServiceError::Logic)
    }
}

impl<L: StartupToolRecoveryLogicPort> TurnService<L> {
    /// Reconciles every durable terminal host receipt against canonical
    /// nonterminal tool dispatches before the daemon accepts connections.
    ///
    /// # Errors
    ///
    /// Returns a translated business failure when a receipt is corrupt,
    /// mismatched, or cannot be committed.
    pub async fn recover_startup_tools(
        &self,
    ) -> Result<ServiceStartupRecoveryResult, TurnServiceError> {
        self.logic
            .recover_startup_tools(RecoverStartupToolsCommand {
                sessions_root: self.sessions_root.clone(),
            })
            .await
            .map(|result| ServiceStartupRecoveryResult {
                receipt_count: result.receipt_count,
                reconciled_count: result.reconciled_count,
                already_terminal_count: result.already_terminal_count,
                deferred_approval_count: result.deferred_approval_count,
                orphaned_count: result.orphaned_count,
            })
            .map_err(TurnServiceError::Logic)
    }
}

/// Full daemon service assembled from independent core and turn logic ports.
#[derive(Clone)]
pub struct RuntimeDaemonService<C, T> {
    core: RuntimeService<C>,
    turns: TurnService<T>,
    scheduler_completion_delay: std::time::Duration,
    plugin_management: Option<Arc<dyn PluginManagementLogicPort>>,
}

enum ScheduledRecoveryState {
    NotStarted,
    Succeeded,
    AwaitingApproval(String),
    Failed,
    Indeterminate,
}

struct ScheduledRecoveryClassification {
    state: ScheduledRecoveryState,
    reconciliation_sequence: Option<agentmod_primitives::Sequence>,
}

impl<C, T> RuntimeDaemonService<C, T> {
    #[must_use]
    pub const fn new(core: RuntimeService<C>, turns: TurnService<T>) -> Self {
        Self {
            core,
            turns,
            scheduler_completion_delay: std::time::Duration::ZERO,
            plugin_management: None,
        }
    }

    /// Adds a completion delay used only by crash-injection validation.
    #[must_use]
    pub const fn with_scheduler_completion_delay(mut self, delay: std::time::Duration) -> Self {
        self.scheduler_completion_delay = delay;
        self
    }

    /// Adds the plugin lifecycle management adapter below the frontend layer.
    #[must_use]
    pub fn with_plugin_management(
        mut self,
        plugin_management: Arc<dyn PluginManagementLogicPort>,
    ) -> Self {
        self.plugin_management = Some(plugin_management);
        self
    }

    async fn handle_plugin_management(
        &self,
        plugin_management: &dyn PluginManagementLogicPort,
        request: &RuntimeRequest,
    ) -> Result<RuntimeResponse, String> {
        match request {
            RuntimeRequest::PluginList { session_id } => {
                let plugins = plugin_management
                    .list_plugins(session_id.clone())
                    .await
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(to_wire_plugin_projection)
                    .collect();
                Ok(RuntimeResponse::PluginListed { plugins })
            }
            RuntimeRequest::PluginInspect {
                session_id,
                plugin_id,
            } => {
                let plugin = plugin_management
                    .inspect_plugin(session_id.clone(), plugin_id.clone())
                    .await
                    .map_err(|error| error.to_string())
                    .map(to_wire_plugin_projection)?;
                Ok(RuntimeResponse::PluginInspected { plugin })
            }
            RuntimeRequest::PluginDisable {
                session_id,
                plugin_id,
            } => {
                let audit = plugin_management
                    .disable_plugin(session_id.clone(), plugin_id.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(RuntimeResponse::PluginLifecycleChanged {
                    plugin_id: plugin_id.clone(),
                    state: "disabled".into(),
                    outcome: audit.outcome,
                })
            }
            RuntimeRequest::PluginQuarantine {
                session_id,
                plugin_id,
                reason,
            } => {
                let audit = plugin_management
                    .quarantine_plugin(session_id.clone(), plugin_id.clone(), reason.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(RuntimeResponse::PluginLifecycleChanged {
                    plugin_id: plugin_id.clone(),
                    state: "quarantined".into(),
                    outcome: audit.outcome,
                })
            }
            RuntimeRequest::PluginUnquarantine {
                session_id,
                plugin_id,
            } => {
                let audit = plugin_management
                    .unquarantine_plugin(session_id.clone(), plugin_id.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(RuntimeResponse::PluginLifecycleChanged {
                    plugin_id: plugin_id.clone(),
                    state: "active".into(),
                    outcome: audit.outcome,
                })
            }
            RuntimeRequest::PluginReload {
                session_id,
                plugin_id,
            } => {
                let audit = plugin_management
                    .reload_plugin(session_id.clone(), plugin_id.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(RuntimeResponse::PluginLifecycleChanged {
                    plugin_id: plugin_id.clone(),
                    state: "reloaded".into(),
                    outcome: audit.outcome,
                })
            }
            RuntimeRequest::PluginHealth { session_id } => {
                let health = plugin_management
                    .host_health(session_id.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(RuntimeResponse::PluginHealthResult {
                    loaded: health.loaded,
                    running: health.running,
                    observer_dropped: health.observer_dropped,
                    pending_deliveries: health.pending_deliveries,
                })
            }
            RuntimeRequest::PluginAudits { session_id } => {
                let audits = plugin_management
                    .host_audits(session_id.clone())
                    .await
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(to_wire_plugin_audit)
                    .collect();
                Ok(RuntimeResponse::PluginAuditsResult { audits })
            }
            _ => Err(String::from("unsupported plugin management request")),
        }
    }
}

fn is_plugin_management_request(request: &RuntimeRequest) -> bool {
    matches!(
        request,
        RuntimeRequest::PluginList { .. }
            | RuntimeRequest::PluginInspect { .. }
            | RuntimeRequest::PluginDisable { .. }
            | RuntimeRequest::PluginQuarantine { .. }
            | RuntimeRequest::PluginUnquarantine { .. }
            | RuntimeRequest::PluginReload { .. }
            | RuntimeRequest::PluginHealth { .. }
            | RuntimeRequest::PluginAudits { .. }
    )
}

fn to_wire_plugin_projection(
    projection: PluginLifecycleProjection,
) -> agentmod_runtime_protocol::RuntimePluginProjection {
    agentmod_runtime_protocol::RuntimePluginProjection {
        plugin_id: projection.plugin_id,
        class: projection.class,
        category: projection.category,
        version: projection.version,
        status: projection.status,
        node_executors: projection.node_executors,
        memory_scopes: projection.memory_scopes.into_iter().collect(),
        compaction_strategy: projection.compaction_strategy,
        context_transforms: projection.context_transforms,
        observer_delivery: projection.observer_delivery,
        timeout_ms: projection.timeout_ms,
    }
}

fn to_wire_plugin_audit(audit: PluginAuditRecord) -> agentmod_runtime_protocol::RuntimePluginAudit {
    agentmod_runtime_protocol::RuntimePluginAudit {
        plugin_id: audit.plugin_id,
        invocation_id: audit.invocation_id,
        operation: audit.operation,
        outcome: audit.outcome,
        attempts: audit.attempts,
    }
}

impl<C, T> RuntimeDaemonService<C, T>
where
    C: agentmod_runtime_logic::RuntimeLogicPort
        + agentmod_runtime_logic::registry::SessionRegistryLogicPort
        + agentmod_runtime_logic::history::SessionHistoryLogicPort
        + agentmod_runtime_logic::style::SessionStyleLogicPort
        + agentmod_runtime_logic::harness_registry::HarnessRegistryLogicPort
        + agentmod_runtime_logic::scheduler::RuntimeScheduleLogicPort,
    T: TurnLogicPort
        + CommittedEventObserverLogicPort
        + DeferredTurnLogicPort
        + ScheduledRecoveryLogicPort,
{
    /// Reconciles durable scheduler claims before the runtime accepts clients.
    ///
    /// Claims with no canonical `scheduler.fired` event are safe to execute:
    /// the event is committed before provider or tool dispatch. Canonically
    /// terminal claims are finalized at the worker. Ambiguous in-flight claims
    /// fail closed and are never redispatched.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when pending claims or canonical history
    /// cannot be read.
    pub async fn recover_pending_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<RuntimeScheduledRun>, String> {
        let RuntimeResponse::ScheduledExecutions { executions } = self
            .core
            .handle_schedule_wire(&RuntimeRequest::ListPendingScheduledExecutions { limit })
            .map_err(|error| error.to_string())?
        else {
            return Err(String::from(
                "runtime scheduler returned an invalid pending response",
            ));
        };
        let mut safe_to_start = Vec::new();
        let mut results = Vec::new();
        for execution in executions {
            let service_execution = ServiceScheduledExecution {
                execution_id: execution.execution_id,
                scheduled_for_ms: execution.scheduled_for_ms,
                claimed_at_ms: execution.claimed_at_ms,
                schedule: crate::from_wire_schedule(execution.schedule),
            };
            let classification = self.classify_scheduled_recovery(&service_execution)?;
            match classification.state {
                ScheduledRecoveryState::NotStarted => safe_to_start.push(service_execution),
                ScheduledRecoveryState::Succeeded => {
                    let sequence = self.ensure_scheduled_reconciliation(
                        &service_execution,
                        "succeeded",
                        None,
                        classification.reconciliation_sequence,
                    )?;
                    let terminal =
                        self.complete_scheduled_claim(&service_execution.execution_id, true);
                    results.push(recovered_run(
                        service_execution,
                        terminal,
                        true,
                        Some(sequence),
                        None,
                    ));
                }
                ScheduledRecoveryState::AwaitingApproval(continuation_id) => {
                    let sequence = self.ensure_scheduled_reconciliation(
                        &service_execution,
                        "awaiting_approval",
                        Some(continuation_id.clone()),
                        classification.reconciliation_sequence,
                    )?;
                    let mut run =
                        recovered_run(service_execution, false, false, Some(sequence), None);
                    run.awaiting_continuation = Some(continuation_id);
                    results.push(run);
                }
                ScheduledRecoveryState::Failed => {
                    let sequence = self.ensure_scheduled_reconciliation(
                        &service_execution,
                        "failed",
                        None,
                        classification.reconciliation_sequence,
                    )?;
                    let terminal =
                        self.complete_scheduled_claim(&service_execution.execution_id, false);
                    results.push(recovered_run(
                        service_execution,
                        terminal,
                        false,
                        Some(sequence),
                        None,
                    ));
                }
                ScheduledRecoveryState::Indeterminate => {
                    let sequence = self.ensure_scheduled_reconciliation(
                        &service_execution,
                        "indeterminate_failed",
                        None,
                        classification.reconciliation_sequence,
                    )?;
                    let terminal =
                        self.complete_scheduled_claim(&service_execution.execution_id, false);
                    results.push(recovered_run(
                        service_execution,
                        terminal,
                        false,
                        Some(sequence),
                        Some(String::from(
                            "interrupted scheduled execution was not redispatched",
                        )),
                    ));
                }
            }
        }
        results.extend(self.execute_scheduled_claims(safe_to_start, true).await);
        Ok(results)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the bounded canonical scan keeps execution-boundary and terminal precedence visible"
    )]
    fn classify_scheduled_recovery(
        &self,
        execution: &ServiceScheduledExecution,
    ) -> Result<ScheduledRecoveryClassification, String> {
        let mut after = None;
        let mut fired = false;
        let mut observed_state = None;
        loop {
            let previous_after = after;
            let page = self
                .core
                .subscribe_session(crate::ServiceSubscribeSessionRequest {
                    session_id: execution.schedule.session_id,
                    after,
                    limit: 1_024,
                })
                .map_err(|error| error.to_string())?;
            for event in page.events {
                after = Some(event.sequence);
                if event.event_type == "scheduler.fired" {
                    let matches_execution = event
                        .payload
                        .get("payload")
                        .and_then(|payload| payload.get("execution_id"))
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == execution.execution_id);
                    if matches_execution {
                        fired = true;
                    } else if fired {
                        return Ok(ScheduledRecoveryClassification {
                            state: observed_state.unwrap_or(ScheduledRecoveryState::Indeterminate),
                            reconciliation_sequence: None,
                        });
                    }
                    continue;
                }
                if !fired {
                    continue;
                }
                match event.event_type.as_str() {
                    "scheduler.delivery_reconciled" => {
                        let Some(payload) = event.payload.get("payload") else {
                            continue;
                        };
                        let matches_execution = payload
                            .get("execution_id")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == execution.execution_id);
                        let matches_schedule = payload
                            .get("schedule_id")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == execution.schedule.schedule_id);
                        if !matches_execution || !matches_schedule {
                            continue;
                        }
                        let state = match payload.get("outcome").and_then(Value::as_str) {
                            Some("succeeded") => ScheduledRecoveryState::Succeeded,
                            Some("failed" | "indeterminate_failed") => {
                                ScheduledRecoveryState::Failed
                            }
                            Some("awaiting_approval") => {
                                let continuation_id = payload
                                    .get("continuation_id")
                                    .and_then(Value::as_str)
                                    .unwrap_or("unknown")
                                    .to_owned();
                                ScheduledRecoveryState::AwaitingApproval(continuation_id)
                            }
                            _ => return Err(String::from("invalid scheduler recovery outcome")),
                        };
                        return Ok(ScheduledRecoveryClassification {
                            state,
                            reconciliation_sequence: Some(event.sequence),
                        });
                    }
                    "model.response_completed" => {
                        observed_state = Some(ScheduledRecoveryState::Succeeded);
                    }
                    "model.request_failed"
                    | "model.request_cancelled"
                    | "session.failed"
                    | "session.cancelled" => {
                        observed_state = Some(ScheduledRecoveryState::Failed);
                    }
                    "approval.requested" => {
                        let continuation_id = event
                            .payload
                            .get("payload")
                            .and_then(|payload| payload.get("continuation_id"))
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned();
                        observed_state =
                            Some(ScheduledRecoveryState::AwaitingApproval(continuation_id));
                    }
                    _ => {}
                }
            }
            if !page.has_more {
                break;
            }
            if after == previous_after {
                return Err(String::from(
                    "scheduler recovery history cursor did not advance",
                ));
            }
        }
        Ok(ScheduledRecoveryClassification {
            state: if fired {
                observed_state.unwrap_or(ScheduledRecoveryState::Indeterminate)
            } else {
                ScheduledRecoveryState::NotStarted
            },
            reconciliation_sequence: None,
        })
    }

    fn ensure_scheduled_reconciliation(
        &self,
        execution: &ServiceScheduledExecution,
        outcome: &str,
        continuation_id: Option<String>,
        existing: Option<agentmod_primitives::Sequence>,
    ) -> Result<agentmod_primitives::Sequence, String> {
        existing.map_or_else(
            || {
                self.turns
                    .record_scheduled_recovery(ServiceRecordScheduledRecoveryRequest {
                        session_id: execution.schedule.session_id.to_string(),
                        execution_id: execution.execution_id.clone(),
                        schedule_id: execution.schedule.schedule_id.clone(),
                        outcome: outcome.to_owned(),
                        continuation_id,
                    })
                    .map_err(|error| error.to_string())
            },
            Ok,
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the service maps prompt and typed continuation claims into one uniform terminal result"
    )]
    async fn execute_scheduled_claims(
        &self,
        executions: Vec<ServiceScheduledExecution>,
        allow_resumed_recovery: bool,
    ) -> Vec<RuntimeScheduledRun> {
        let mut runs = Vec::with_capacity(executions.len());
        for execution in executions {
            let execution_id = execution.execution_id;
            let schedule = execution.schedule;
            let turn = match schedule.payload {
                ServiceSchedulePayload::Prompt { prompt } => self
                    .turns
                    .run_scheduled_turn(ServiceRunScheduledTurnRequest {
                        execution_id: execution_id.clone(),
                        schedule_id: schedule.schedule_id.clone(),
                        scheduled_for_ms: execution.scheduled_for_ms,
                        turn: ServiceRunTurnRequest {
                            session_id: schedule.session_id.to_string(),
                            prompt,
                            provider: schedule.provider,
                            model: schedule.model,
                            options: serde_json::json!({
                                "scheduled_execution_id": execution_id,
                                "token_budget": schedule.token_budget,
                                "cost_budget_micros": schedule.cost_budget_micros,
                                "permission_policy": schedule.permission_policy,
                                "style": schedule.style,
                                "workspace": schedule.workspace
                            }),
                            cancellation_id: cancellation_id_from_execution(&execution_id),
                        },
                    })
                    .await
                    .map(Some),
                ServiceSchedulePayload::Continuation { continuation_id } => {
                    let wake = self
                        .turns
                        .wake_scheduled_turn(ServiceWakeScheduledTurnRequest {
                            session_id: schedule.session_id.to_string(),
                            continuation_id,
                            execution_id: execution_id.clone(),
                            schedule_id: schedule.schedule_id.clone(),
                            scheduled_for_ms: execution.scheduled_for_ms,
                            proof: wake_proof_from_schedule(
                                &schedule.trigger,
                                execution.claimed_at_ms,
                            ),
                            allow_resumed_recovery,
                        })
                        .await;
                    match wake {
                        Ok(result) if !result.transitioned => Ok(None),
                        Ok(result) => Ok(result.turn),
                        Err(error) => Err(error),
                    }
                }
            };
            match turn {
                Ok(Some(result)) if result.awaiting_continuation.is_some() => {
                    runs.push(RuntimeScheduledRun {
                        execution_id,
                        schedule_id: schedule.schedule_id,
                        terminal: false,
                        succeeded: false,
                        last_committed_sequence: Some(result.last_committed_sequence),
                        awaiting_continuation: result.awaiting_continuation,
                        error: None,
                    });
                }
                Ok(Some(result)) => {
                    if !self.scheduler_completion_delay.is_zero() {
                        tokio::time::sleep(self.scheduler_completion_delay).await;
                    }
                    let terminal = self.complete_scheduled_claim(&execution_id, true);
                    runs.push(RuntimeScheduledRun {
                        execution_id,
                        schedule_id: schedule.schedule_id,
                        terminal,
                        succeeded: true,
                        last_committed_sequence: Some(result.last_committed_sequence),
                        awaiting_continuation: None,
                        error: None,
                    });
                }
                Ok(None) => {
                    if !self.scheduler_completion_delay.is_zero() {
                        tokio::time::sleep(self.scheduler_completion_delay).await;
                    }
                    let terminal = self.complete_scheduled_claim(&execution_id, true);
                    runs.push(RuntimeScheduledRun {
                        execution_id,
                        schedule_id: schedule.schedule_id,
                        terminal,
                        succeeded: true,
                        last_committed_sequence: None,
                        awaiting_continuation: None,
                        error: None,
                    });
                }
                Err(error) => {
                    if !self.scheduler_completion_delay.is_zero() {
                        tokio::time::sleep(self.scheduler_completion_delay).await;
                    }
                    let terminal = self.complete_scheduled_claim(&execution_id, false);
                    runs.push(RuntimeScheduledRun {
                        execution_id,
                        schedule_id: schedule.schedule_id,
                        terminal,
                        succeeded: false,
                        last_committed_sequence: None,
                        awaiting_continuation: None,
                        error: Some(error.to_string()),
                    });
                }
            }
        }
        runs
    }

    fn complete_scheduled_claim(&self, execution_id: &str, succeeded: bool) -> bool {
        let response =
            self.core
                .handle_schedule_wire(&RuntimeRequest::CompleteScheduledExecution {
                    execution_id: execution_id.to_owned(),
                    succeeded,
                });
        matches!(
            response,
            Ok(RuntimeResponse::ScheduledExecutionCompleted { changed: true })
        )
    }

    fn collect_scheduled_matches(
        &self,
        event: &crate::ServiceSessionEvent,
        executions: &mut std::collections::BTreeMap<String, ServiceScheduledExecution>,
    ) {
        match self
            .core
            .fire_runtime_event(event.event_id.to_string(), event.event_type.clone())
        {
            Ok(values) => {
                for execution in values {
                    executions.insert(execution.execution_id.clone(), execution);
                }
            }
            Err(_) => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "runtime.scheduler_observation_failed",
                        "source_event_id": event.event_id,
                        "source_event_type": event.event_type,
                        "reason": "runtime_event_delivery_unavailable"
                    })
                );
            }
        }
        if event.event_type != "tool.output_observed" {
            return;
        }
        let Some(payload) = event.payload.get("payload") else {
            return;
        };
        let (Some(process_id), Some(output)) = (
            payload.get("process_id").and_then(Value::as_str),
            payload.get("content").and_then(Value::as_str),
        ) else {
            return;
        };
        let output_id = match (
            payload.get("source_stream").and_then(Value::as_str),
            payload.get("source_offset").and_then(Value::as_u64),
            payload.get("source_end").and_then(Value::as_u64),
        ) {
            (Some(stream), Some(start), Some(end)) => {
                format!("process-output:{process_id}:{stream}:{start}:{end}")
            }
            _ => event.event_id.to_string(),
        };
        match self
            .core
            .fire_process_output(output_id, process_id.to_owned(), output.to_owned())
        {
            Ok(values) => {
                for execution in values {
                    executions.insert(execution.execution_id.clone(), execution);
                }
            }
            Err(_) => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "runtime.scheduler_observation_failed",
                        "source_event_id": event.event_id,
                        "source_event_type": event.event_type,
                        "reason": "process_output_delivery_unavailable"
                    })
                );
            }
        }
    }

    async fn observe_committed_range(
        &self,
        session_id: agentmod_primitives::SessionId,
        first: agentmod_primitives::Sequence,
        last: agentmod_primitives::Sequence,
    ) {
        let mut after = first
            .get()
            .checked_sub(1)
            .and_then(|value| (value > 0).then_some(value))
            .and_then(|value| agentmod_primitives::Sequence::new(value).ok());
        let mut executions = std::collections::BTreeMap::new();
        let mut committed_events = Vec::new();
        loop {
            let page = self
                .core
                .subscribe_session(crate::ServiceSubscribeSessionRequest {
                    session_id,
                    after,
                    limit: 1_024,
                });
            let Ok(page) = page else {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "runtime.scheduler_observation_failed",
                        "session_id": session_id,
                        "reason": "canonical_event_page_unavailable"
                    })
                );
                return;
            };
            let previous_after = after;
            for event in page
                .events
                .into_iter()
                .take_while(|event| event.sequence <= last)
            {
                after = Some(event.sequence);
                self.collect_scheduled_matches(&event, &mut executions);
                committed_events.push(event);
            }
            if after.is_some_and(|sequence| sequence >= last) {
                break;
            }
            if after == previous_after || !page.has_more {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "runtime.scheduler_observation_failed",
                        "session_id": session_id,
                        "reason": "canonical_event_range_incomplete",
                        "last_observed_sequence": after,
                        "expected_last_sequence": last,
                    })
                );
                break;
            }
        }
        let _ = self
            .execute_scheduled_claims(executions.into_values().collect(), false)
            .await;
        if let Err(error) = self
            .turns
            .observe_committed_events(session_id, committed_events)
            .await
        {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "runtime.plugin_observation_failed",
                    "session_id": session_id,
                    "reason": error.to_string(),
                })
            );
        }
    }
}

#[async_trait]
impl<C, T> crate::local_rpc::RuntimeWireEndpoint for RuntimeDaemonService<C, T>
where
    C: agentmod_runtime_logic::RuntimeLogicPort
        + agentmod_runtime_logic::registry::SessionRegistryLogicPort
        + agentmod_runtime_logic::history::SessionHistoryLogicPort
        + agentmod_runtime_logic::style::SessionStyleLogicPort
        + agentmod_runtime_logic::harness_registry::HarnessRegistryLogicPort
        + agentmod_runtime_logic::scheduler::RuntimeScheduleLogicPort
        + Clone
        + Send
        + Sync
        + 'static,
    T: TurnLogicPort
        + ApprovalTurnLogicPort
        + CancelTurnLogicPort
        + CommittedEventObserverLogicPort
        + DeferredTurnLogicPort
        + ScheduledRecoveryLogicPort
        + Clone
        + 'static,
{
    #[allow(
        clippy::too_many_lines,
        reason = "the composite endpoint explicitly maps core, turn, approval, and schedule routes"
    )]
    async fn handle_runtime_request(
        &self,
        request: &RuntimeRequest,
    ) -> Result<RuntimeResponse, String> {
        if let RuntimeRequest::ResolveApproval {
            session_id,
            continuation_id,
            approved,
            resume_after_resolution,
        } = request
        {
            self.core
                .validate_session_style_compatibility(*session_id)
                .map_err(|error| error.to_string())?;
            let (transitioned, events, last_sequence, awaiting_continuation) = self
                .turns
                .resolve_approval(
                    session_id.to_string(),
                    continuation_id.clone(),
                    *approved,
                    *resume_after_resolution,
                )
                .await
                .map_err(|error| error.to_string())?;
            return Ok(RuntimeResponse::ApprovalResolved {
                transitioned,
                events: events.into_iter().map(to_wire_event).collect(),
                last_committed_sequence: transitioned.then_some(last_sequence),
                awaiting_continuation,
            });
        }
        if let RuntimeRequest::Cancel {
            cancellation_id,
            reason,
        } = request
        {
            self.turns
                .cancel_turn(cancellation_id.to_string(), reason.clone())
                .await
                .map_err(|error| error.to_string())?;
            return Ok(RuntimeResponse::Cancelled);
        }
        if let RuntimeRequest::CreateDeferredTurn {
            session_id,
            continuation_id,
            schedule_id,
            prompt,
            workspace,
            provider,
            model,
            options,
            style,
            cancellation_id,
            trigger,
            expires_at_ms,
        } = request
        {
            self.core
                .validate_session_style_compatibility(*session_id)
                .map_err(|error| error.to_string())?;
            let wake_condition = wake_condition_from_wire(trigger)
                .ok_or_else(|| String::from("interval deferred continuations are unsupported"))?;
            self.turns
                .create_deferred_turn(ServiceCreateDeferredTurnRequest {
                    session_id: session_id.to_string(),
                    continuation_id: continuation_id.clone(),
                    schedule_id: schedule_id.clone(),
                    prompt: prompt.clone(),
                    workspace: workspace.clone(),
                    provider: provider.clone(),
                    model: model.clone(),
                    options: options.clone(),
                    style: style.clone(),
                    cancellation_id: cancellation_id.to_string(),
                    wake_condition,
                    expires_at_ms: *expires_at_ms,
                })
                .map_err(|error| error.to_string())?;
            return Ok(RuntimeResponse::DeferredTurnCreated {
                continuation_id: continuation_id.clone(),
            });
        }
        if let RuntimeRequest::RunDueSchedules { limit } = request {
            let RuntimeResponse::ScheduledExecutions { executions } = self
                .core
                .handle_schedule_wire(&RuntimeRequest::ClaimDueSchedules { limit: *limit })
                .map_err(|error| error.to_string())?
            else {
                return Err(String::from(
                    "runtime scheduler returned an invalid claim response",
                ));
            };
            let executions = executions
                .into_iter()
                .map(|execution| ServiceScheduledExecution {
                    execution_id: execution.execution_id,
                    scheduled_for_ms: execution.scheduled_for_ms,
                    claimed_at_ms: execution.claimed_at_ms,
                    schedule: crate::from_wire_schedule(execution.schedule),
                })
                .collect();
            let runs = self.execute_scheduled_claims(executions, false).await;
            return Ok(RuntimeResponse::ScheduledRuns { runs });
        }
        if let Some(plugin_management) = &self.plugin_management
            && is_plugin_management_request(request)
        {
            return self
                .handle_plugin_management(plugin_management.as_ref(), request)
                .await;
        }
        let RuntimeRequest::RunTurn {
            session_id,
            prompt,
            provider,
            model,
            options,
            cancellation_id,
        } = request
        else {
            if matches!(
                request,
                RuntimeRequest::UpsertSchedule { .. }
                    | RuntimeRequest::RemoveSchedule { .. }
                    | RuntimeRequest::ListSchedules { .. }
                    | RuntimeRequest::ClaimDueSchedules { .. }
                    | RuntimeRequest::ListPendingScheduledExecutions { .. }
                    | RuntimeRequest::CompleteScheduledExecution { .. }
            ) {
                return self
                    .core
                    .handle_schedule_wire(request)
                    .map_err(|error| error.to_string());
            }
            return self
                .core
                .handle_wire(request)
                .map_err(|error| error.to_string());
        };
        self.core
            .validate_session_style_compatibility(*session_id)
            .map_err(|error| error.to_string())?;
        let response = self
            .turns
            .run_turn(ServiceRunTurnRequest {
                session_id: session_id.to_string(),
                prompt: prompt.clone(),
                provider: provider.clone(),
                model: model.clone(),
                options: options.clone(),
                cancellation_id: cancellation_id.to_string(),
            })
            .await
            .map_err(|error| error.to_string())?;
        self.observe_committed_range(
            *session_id,
            response.first_committed_sequence,
            response.last_committed_sequence,
        )
        .await;
        Ok(RuntimeResponse::Turn {
            events: response.events.into_iter().map(to_wire_event).collect(),
            first_committed_sequence: response.first_committed_sequence,
            last_committed_sequence: response.last_committed_sequence,
            awaiting_continuation: response.awaiting_continuation,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "composite service maps two distinct bounded stream endpoint families"
    )]
    async fn handle_runtime_stream(
        &self,
        request: &RuntimeRequest,
    ) -> Result<crate::local_rpc::RuntimeEndpointStream, String> {
        if let RuntimeRequest::Subscribe {
            session_id,
            after,
            limit,
        } = request
        {
            let page = self
                .core
                .subscribe_session(crate::ServiceSubscribeSessionRequest {
                    session_id: *session_id,
                    after: *after,
                    limit: *limit,
                })
                .map_err(|error| error.to_string())?;
            if page.events.is_empty() {
                return Ok(crate::local_rpc::RuntimeEndpointStream::single(
                    RuntimeResponse::SessionEvents {
                        events: Vec::new(),
                        head_sequence: page.head_sequence,
                        last_delivered_sequence: page.last_delivered_sequence,
                        has_more: page.has_more,
                    },
                ));
            }
            let (sender, receiver) = mpsc::channel(16);
            tokio::spawn(async move {
                for event in page.events {
                    if sender
                        .send(Ok(crate::local_rpc::RuntimeEndpointFrame {
                            response: RuntimeResponse::SessionEvent {
                                event_id: Some(event.event_id),
                                sequence: event.sequence,
                                event_type: event.event_type,
                                payload: event.payload,
                            },
                            terminal: false,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = sender
                    .send(Ok(crate::local_rpc::RuntimeEndpointFrame {
                        response: RuntimeResponse::SubscriptionComplete {
                            head_sequence: page.head_sequence,
                            last_delivered_sequence: page.last_delivered_sequence,
                            has_more: page.has_more,
                        },
                        terminal: true,
                    }))
                    .await;
            });
            return Ok(crate::local_rpc::RuntimeEndpointStream::from_receiver(
                receiver,
            ));
        }
        let RuntimeRequest::RunTurn {
            session_id,
            prompt,
            provider,
            model,
            options,
            cancellation_id,
        } = request
        else {
            return self
                .handle_runtime_request(request)
                .await
                .map(crate::local_rpc::RuntimeEndpointStream::single);
        };
        self.core
            .validate_session_style_compatibility(*session_id)
            .map_err(|error| error.to_string())?;
        let mut turn = self
            .turns
            .run_turn_stream(ServiceRunTurnRequest {
                session_id: session_id.to_string(),
                prompt: prompt.clone(),
                provider: provider.clone(),
                model: model.clone(),
                options: options.clone(),
                cancellation_id: cancellation_id.to_string(),
            })
            .await
            .map_err(|error| error.to_string())?;
        let observer = self.clone();
        let observed_session = *session_id;
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(item) = turn.next().await {
                let item = match item {
                    Ok(item) => item,
                    Err(error) => {
                        let _ = sender.send(Err(error.to_string())).await;
                        break;
                    }
                };
                match item {
                    ServiceTurnStreamItem::Event {
                        event,
                        committed_sequence,
                    } => {
                        let frame = crate::local_rpc::RuntimeEndpointFrame {
                            response: RuntimeResponse::TurnEvent {
                                event: to_wire_event(event),
                                committed_sequence,
                            },
                            terminal: false,
                        };
                        if sender.send(Ok(frame)).await.is_err() {
                            break;
                        }
                    }
                    ServiceTurnStreamItem::Complete {
                        first_committed_sequence,
                        last_committed_sequence,
                        awaiting_continuation,
                    } => {
                        let frame = crate::local_rpc::RuntimeEndpointFrame {
                            response: RuntimeResponse::TurnComplete {
                                first_committed_sequence,
                                last_committed_sequence,
                                awaiting_continuation,
                            },
                            terminal: true,
                        };
                        let _ = sender.send(Ok(frame)).await;
                        observer
                            .observe_committed_range(
                                observed_session,
                                first_committed_sequence,
                                last_committed_sequence,
                            )
                            .await;
                        break;
                    }
                }
            }
        });
        Ok(crate::local_rpc::RuntimeEndpointStream::from_receiver(
            receiver,
        ))
    }
}

fn recovered_run(
    execution: ServiceScheduledExecution,
    terminal: bool,
    succeeded: bool,
    last_committed_sequence: Option<agentmod_primitives::Sequence>,
    error: Option<String>,
) -> RuntimeScheduledRun {
    RuntimeScheduledRun {
        execution_id: execution.execution_id,
        schedule_id: execution.schedule.schedule_id,
        terminal,
        succeeded,
        last_committed_sequence,
        awaiting_continuation: None,
        error,
    }
}

fn wake_condition_from_wire(
    trigger: &RuntimeScheduleTrigger,
) -> Option<ServiceContinuationWakeCondition> {
    match trigger {
        RuntimeScheduleTrigger::AtMillis(value) => {
            Some(ServiceContinuationWakeCondition::AtMillis(*value))
        }
        RuntimeScheduleTrigger::Interval { .. } => None,
        RuntimeScheduleTrigger::RuntimeEvent { event_type } => {
            Some(ServiceContinuationWakeCondition::RuntimeEvent {
                event_type: event_type.clone(),
            })
        }
        RuntimeScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => Some(ServiceContinuationWakeCondition::ProcessOutput {
            process_id: process_id.clone(),
            contains: contains.clone(),
        }),
    }
}

fn wake_proof_from_schedule(
    trigger: &crate::ServiceScheduleTrigger,
    scheduled_for_ms: i64,
) -> ServiceContinuationWakeProof {
    match trigger {
        crate::ServiceScheduleTrigger::AtMillis(_)
        | crate::ServiceScheduleTrigger::Interval { .. } => {
            ServiceContinuationWakeProof::AtMillis(scheduled_for_ms)
        }
        crate::ServiceScheduleTrigger::RuntimeEvent { event_type } => {
            ServiceContinuationWakeProof::RuntimeEvent {
                event_type: event_type.clone(),
                observed_at_ms: scheduled_for_ms,
            }
        }
        crate::ServiceScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => ServiceContinuationWakeProof::ProcessOutput {
            process_id: process_id.clone(),
            contains: contains.clone(),
            observed_at_ms: scheduled_for_ms,
        },
    }
}

fn to_logic_wake_condition(value: ServiceContinuationWakeCondition) -> ContinuationWakeCondition {
    match value {
        ServiceContinuationWakeCondition::AtMillis(value) => {
            ContinuationWakeCondition::At(agentmod_primitives::TimestampMillis::new(value))
        }
        ServiceContinuationWakeCondition::RuntimeEvent { event_type } => {
            ContinuationWakeCondition::RuntimeEvent {
                event_type,
                selector: None,
            }
        }
        ServiceContinuationWakeCondition::ProcessOutput {
            process_id,
            contains,
        } => ContinuationWakeCondition::ProcessOutput {
            process_id,
            pattern: contains,
        },
    }
}

fn to_logic_wake_proof(value: ServiceContinuationWakeProof) -> ContinuationWakeProof {
    match value {
        ServiceContinuationWakeProof::AtMillis(value) => {
            ContinuationWakeProof::At(agentmod_primitives::TimestampMillis::new(value))
        }
        ServiceContinuationWakeProof::RuntimeEvent {
            event_type,
            observed_at_ms,
        } => ContinuationWakeProof::RuntimeEvent {
            event_type,
            observed_at: agentmod_primitives::TimestampMillis::new(observed_at_ms),
        },
        ServiceContinuationWakeProof::ProcessOutput {
            process_id,
            contains,
            observed_at_ms,
        } => ContinuationWakeProof::ProcessOutput {
            process_id,
            pattern: contains,
            observed_at: agentmod_primitives::TimestampMillis::new(observed_at_ms),
        },
    }
}

fn map_event(event: ProviderEvent) -> ServiceTurnEvent {
    match event {
        ProviderEvent::Started => ServiceTurnEvent::Started,
        ProviderEvent::Text(value) => ServiceTurnEvent::Text(value),
        ProviderEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => ServiceTurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        ProviderEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => ServiceTurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        ProviderEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        } => ServiceTurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        },
        ProviderEvent::Cancelled => ServiceTurnEvent::Cancelled,
        ProviderEvent::Failed {
            code,
            message,
            retryable,
        } => ServiceTurnEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

fn cancellation_id_from_execution(execution_id: &str) -> String {
    format!(
        "{}-{}-{}-{}-{}",
        &execution_id[0..8],
        &execution_id[8..12],
        &execution_id[12..16],
        &execution_id[16..20],
        &execution_id[20..32]
    )
}

fn to_wire_event(event: ServiceTurnEvent) -> RuntimeProviderEvent {
    match event {
        ServiceTurnEvent::Started => RuntimeProviderEvent::Started,
        ServiceTurnEvent::Text(text) => RuntimeProviderEvent::Text { text },
        ServiceTurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => RuntimeProviderEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        ServiceTurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => RuntimeProviderEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        ServiceTurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        } => RuntimeProviderEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        },
        ServiceTurnEvent::Cancelled => RuntimeProviderEvent::Cancelled,
        ServiceTurnEvent::Failed {
            code,
            message,
            retryable,
        } => RuntimeProviderEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

#[derive(Debug, Error)]
pub enum TurnServiceError {
    #[error("turn request is invalid")]
    Invalid,
    #[error("turn failed: {0}")]
    Logic(RunTurnError),
}
