//! Durable turn endpoint mapping and composite runtime routing.
#![allow(
    missing_docs,
    reason = "service-local turn records are intentionally boundary-specific"
)]

use std::path::PathBuf;

use agentmod_runtime_logic::{
    harness::ProviderEvent,
    turn::{
        ApprovalTurnLogicPort, CancelTurnCommand, CancelTurnLogicPort, RecoverStartupToolsCommand,
        ResolveTurnApprovalCommand, RunScheduledTurnCommand, RunTurnCommand, RunTurnError,
        RunTurnStream, RunTurnStreamItem, StartupToolRecoveryLogicPort, TurnLogicPort,
    },
};
use agentmod_runtime_protocol::{
    RuntimeProviderEvent, RuntimeRequest, RuntimeResponse, RuntimeScheduledRun,
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
}

impl<C, T> RuntimeDaemonService<C, T> {
    #[must_use]
    pub const fn new(core: RuntimeService<C>, turns: TurnService<T>) -> Self {
        Self { core, turns }
    }
}

impl<C, T> RuntimeDaemonService<C, T>
where
    C: agentmod_runtime_logic::RuntimeLogicPort
        + agentmod_runtime_logic::registry::SessionRegistryLogicPort
        + agentmod_runtime_logic::history::SessionHistoryLogicPort
        + agentmod_runtime_logic::scheduler::RuntimeScheduleLogicPort,
    T: TurnLogicPort,
{
    async fn execute_scheduled_claims(
        &self,
        executions: Vec<ServiceScheduledExecution>,
    ) -> Vec<RuntimeScheduledRun> {
        let mut runs = Vec::with_capacity(executions.len());
        for execution in executions {
            let execution_id = execution.execution_id;
            let schedule = execution.schedule;
            let ServiceSchedulePayload::Prompt { prompt } = schedule.payload else {
                let terminal = self.complete_scheduled_claim(&execution_id, false);
                runs.push(RuntimeScheduledRun {
                    execution_id,
                    schedule_id: schedule.schedule_id,
                    terminal,
                    succeeded: false,
                    last_committed_sequence: None,
                    awaiting_continuation: None,
                    error: Some(String::from(
                        "continuation wake schedules require a deferred-action payload",
                    )),
                });
                continue;
            };
            let turn = self
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
                .await;
            match turn {
                Ok(result) if result.awaiting_continuation.is_some() => {
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
                Ok(result) => {
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
                Err(error) => {
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
            .execute_scheduled_claims(executions.into_values().collect())
            .await;
    }
}

#[async_trait]
impl<C, T> crate::local_rpc::RuntimeWireEndpoint for RuntimeDaemonService<C, T>
where
    C: agentmod_runtime_logic::RuntimeLogicPort
        + agentmod_runtime_logic::registry::SessionRegistryLogicPort
        + agentmod_runtime_logic::history::SessionHistoryLogicPort
        + agentmod_runtime_logic::scheduler::RuntimeScheduleLogicPort
        + Clone
        + Send
        + Sync
        + 'static,
    T: TurnLogicPort + ApprovalTurnLogicPort + CancelTurnLogicPort + Clone + 'static,
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
                    schedule: crate::from_wire_schedule(execution.schedule),
                })
                .collect();
            let runs = self.execute_scheduled_claims(executions).await;
            return Ok(RuntimeResponse::ScheduledRuns { runs });
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
