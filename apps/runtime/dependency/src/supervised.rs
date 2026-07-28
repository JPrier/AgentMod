//! Concrete dependency bundle for the long-running runtime composition root.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, sleep};

use crate::{
    DependencyError, DependencyStorageHealthRequest, DependencyStorageHealthResponse,
    LocalRuntimeDependencies, RuntimeDependencyPort,
    continuation::{
        ContinuationDependencyError, ContinuationDependencyPort, DependencyContinuationRecord,
        DependencyCreateContinuationRequest, DependencyTransitionContinuationRequest,
        DependencyTransitionContinuationResponse, FileContinuationDependency,
    },
    harness::{
        DependencyCommand, DependencyEventStream, DependencyReply, HarnessDependencyError,
        HarnessDependencyPort, ProcessHarnessDependency,
    },
    identity::{
        DependencyAllocateEventIdentityRequest, DependencyEventIdentity,
        EventIdentityDependencyError, EventIdentityDependencyPort, allocate,
    },
    journal::{
        DependencyAppendJournalRequest, DependencyAppendJournalResponse,
        DependencyRecoverJournalRequest, DependencyRecoverJournalResponse,
        DependencyScanJournalRequest, DependencyScanJournalResponse, JournalDependencyError,
        JournalDependencyPort, JsonlJournalDependency,
    },
    process_tool::ProcessCapabilityDependency,
    receipt::ToolReceiptDependency,
    registry::{
        DependencyCreateBranchRequest, DependencyCreateSessionRequest, DependencyCreatedSession,
        DependencyListSessionsRequest, DependencyPrepareSessionRequest, DependencyPreparedSession,
        DependencySessionMetadata, FileSessionCatalogDependency, SessionCatalogDependencyError,
        SessionCatalogDependencyPort,
    },
    scheduler::{
        DependencyRuntimeSchedule, DependencyScheduleStoreResult, DependencyScheduledExecution,
        ProcessSchedulerDependency, RuntimeSchedulerDependencyError,
        RuntimeSchedulerDependencyPort,
    },
    tool::{
        DependencyCancelToolRequest, DependencyToolCommand, DependencyToolEvent,
        ProcessToolHostDependency, ToolHostDependencyError, ToolHostDependencyPort,
    },
};

/// First-party local storage plus one supervised native harness.
#[derive(Clone)]
pub struct SupervisedRuntimeDependencies {
    harness: ProcessHarnessDependency,
    browser: ProcessToolHostDependency,
    filesystem: ProcessToolHostDependency,
    processes: ProcessCapabilityDependency,
    git: ProcessToolHostDependency,
    web: ProcessToolHostDependency,
    lsp: ProcessToolHostDependency,
    mcp: ProcessToolHostDependency,
    receipts: ToolReceiptDependency,
    continuations: FileContinuationDependency,
    scheduler: ProcessSchedulerDependency,
    active_tools: Arc<Mutex<BTreeMap<String, ActiveTool>>>,
}

#[derive(Clone)]
struct ActiveTool {
    session_id: String,
    workspace: PathBuf,
    tool: String,
    cancellation: Arc<ActiveToolCancellation>,
}

struct ActiveToolCancellation {
    state: Mutex<ActiveToolCancellationState>,
    resolved: Notify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveToolCancellationState {
    Idle,
    Pending,
    Resolved(bool),
}

impl ActiveToolCancellation {
    fn new() -> Self {
        Self {
            state: Mutex::new(ActiveToolCancellationState::Idle),
            resolved: Notify::new(),
        }
    }

    async fn begin(&self) -> bool {
        let mut state = self.state.lock().await;
        match *state {
            ActiveToolCancellationState::Idle => {
                *state = ActiveToolCancellationState::Pending;
                true
            }
            ActiveToolCancellationState::Pending | ActiveToolCancellationState::Resolved(_) => {
                false
            }
        }
    }

    async fn finish(&self, confirmed: bool) {
        *self.state.lock().await = ActiveToolCancellationState::Resolved(confirmed);
        self.resolved.notify_waiters();
    }

    async fn wait_for_existing_request(&self) -> bool {
        loop {
            let notified = self.resolved.notified();
            match *self.state.lock().await {
                ActiveToolCancellationState::Idle => return false,
                ActiveToolCancellationState::Resolved(confirmed) => return confirmed,
                ActiveToolCancellationState::Pending => notified.await,
            }
        }
    }
}

impl SupervisedRuntimeDependencies {
    /// Creates a concrete dependency bundle.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root explicitly injects each isolated capability boundary"
    )]
    pub fn new(
        harness: ProcessHarnessDependency,
        browser: ProcessToolHostDependency,
        filesystem: ProcessToolHostDependency,
        processes: ProcessCapabilityDependency,
        git: ProcessToolHostDependency,
        web: ProcessToolHostDependency,
        lsp: ProcessToolHostDependency,
        mcp: ProcessToolHostDependency,
        receipts: ToolReceiptDependency,
        continuations: FileContinuationDependency,
        scheduler: ProcessSchedulerDependency,
    ) -> Self {
        Self {
            harness,
            browser,
            filesystem,
            processes,
            git,
            web,
            lsp,
            mcp,
            receipts,
            continuations,
            scheduler,
            active_tools: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl RuntimeSchedulerDependencyPort for SupervisedRuntimeDependencies {
    fn upsert(
        &self,
        schedule: DependencyRuntimeSchedule,
    ) -> Result<DependencyScheduleStoreResult, RuntimeSchedulerDependencyError> {
        self.scheduler.upsert(schedule)
    }

    fn remove(&self, schedule_id: &str) -> Result<bool, RuntimeSchedulerDependencyError> {
        self.scheduler.remove(schedule_id)
    }

    fn list(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyRuntimeSchedule>, RuntimeSchedulerDependencyError> {
        self.scheduler.list(limit)
    }

    fn claim_due(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.scheduler.claim_due(limit)
    }

    fn list_pending_executions(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.scheduler.list_pending_executions(limit)
    }

    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.scheduler.fire_runtime_event(event_id, event_type)
    }

    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.scheduler
            .fire_process_output(output_id, process_id, output)
    }

    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeSchedulerDependencyError> {
        self.scheduler.complete_execution(execution_id, succeeded)
    }
}

impl ContinuationDependencyPort for SupervisedRuntimeDependencies {
    fn create_continuation(
        &self,
        request: DependencyCreateContinuationRequest,
    ) -> Result<(), ContinuationDependencyError> {
        self.continuations.create_continuation(request)
    }

    fn load_continuation(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<DependencyContinuationRecord, ContinuationDependencyError> {
        self.continuations.load_continuation(session_id, id)
    }

    fn transition_continuation(
        &self,
        request: DependencyTransitionContinuationRequest,
    ) -> Result<DependencyTransitionContinuationResponse, ContinuationDependencyError> {
        self.continuations.transition_continuation(request)
    }
}

impl RuntimeDependencyPort for SupervisedRuntimeDependencies {
    fn check_storage(
        &self,
        request: DependencyStorageHealthRequest,
    ) -> Result<DependencyStorageHealthResponse, DependencyError> {
        LocalRuntimeDependencies.check_storage(request)
    }
}

impl SessionCatalogDependencyPort for SupervisedRuntimeDependencies {
    fn prepare_session(
        &self,
        request: DependencyPrepareSessionRequest,
    ) -> Result<DependencyPreparedSession, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.prepare_session(request)
    }

    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.create_session(request)
    }

    fn create_branch(
        &self,
        request: DependencyCreateBranchRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.create_branch(request)
    }

    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionMetadata>, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.list_sessions(request)
    }
}

impl JournalDependencyPort for SupervisedRuntimeDependencies {
    fn append(
        &self,
        request: DependencyAppendJournalRequest,
    ) -> Result<DependencyAppendJournalResponse, JournalDependencyError> {
        JsonlJournalDependency.append(request)
    }

    fn scan(
        &self,
        request: DependencyScanJournalRequest,
    ) -> Result<DependencyScanJournalResponse, JournalDependencyError> {
        JsonlJournalDependency.scan(request)
    }

    fn recover_tail(
        &self,
        request: DependencyRecoverJournalRequest,
    ) -> Result<DependencyRecoverJournalResponse, JournalDependencyError> {
        JsonlJournalDependency.recover_tail(request)
    }
}

impl EventIdentityDependencyPort for SupervisedRuntimeDependencies {
    fn allocate_event_identity(
        &self,
        _request: DependencyAllocateEventIdentityRequest,
    ) -> Result<DependencyEventIdentity, EventIdentityDependencyError> {
        allocate()
    }
}

#[async_trait]
impl HarnessDependencyPort for SupervisedRuntimeDependencies {
    async fn exchange(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyReply, HarnessDependencyError> {
        self.harness.exchange(command).await
    }

    async fn exchange_events(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyEventStream, HarnessDependencyError> {
        self.harness.exchange_events(command).await
    }

    async fn shutdown(&self) {
        self.harness.shutdown().await;
    }
}

#[async_trait]
impl ToolHostDependencyPort for SupervisedRuntimeDependencies {
    async fn execute(
        &self,
        command: DependencyToolCommand,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError> {
        if let Some(events) = self.receipts.load(&command)? {
            return Ok(events);
        }
        if command.receipt_only {
            return Err(ToolHostDependencyError::ReceiptMissing);
        }
        {
            let mut active = self.active_tools.lock().await;
            if active.contains_key(&command.cancellation_id) {
                return Err(ToolHostDependencyError::InvalidRequest);
            }
            active.insert(
                command.cancellation_id.clone(),
                ActiveTool {
                    session_id: command.session_id.clone(),
                    workspace: command.workspace.clone(),
                    tool: command.tool.clone(),
                    cancellation: Arc::new(ActiveToolCancellation::new()),
                },
            );
        }
        let result = if command.tool.starts_with("browser.") {
            self.browser.execute(command.clone()).await
        } else if command.tool.starts_with("process.") {
            self.processes.execute(command.clone()).await
        } else if command.tool.starts_with("git.") {
            self.git.execute(command.clone()).await
        } else if command.tool.starts_with("web.") || command.tool.starts_with("http.") {
            self.web.execute(command.clone()).await
        } else if command.tool.starts_with("lsp.") {
            self.lsp.execute(command.clone()).await
        } else if command.tool.starts_with("mcp.") {
            self.mcp.execute(command.clone()).await
        } else {
            self.filesystem.execute(command.clone()).await
        };
        let cancellation = self
            .active_tools
            .lock()
            .await
            .get(&command.cancellation_id)
            .map(|active| Arc::clone(&active.cancellation));
        let cancellation_confirmed = match cancellation {
            Some(cancellation) => cancellation.wait_for_existing_request().await,
            None => false,
        };
        self.active_tools
            .lock()
            .await
            .remove(&command.cancellation_id);
        let events = if cancellation_confirmed {
            cancelled_tool_events(&command.call_id, result)
        } else {
            result?
        };
        self.receipts.persist(&command, &events)?;
        if !self.receipts.post_persist_delay().is_zero() {
            tokio::time::sleep(self.receipts.post_persist_delay()).await;
        }
        Ok(events)
    }

    async fn cancel(
        &self,
        request: DependencyCancelToolRequest,
    ) -> Result<bool, ToolHostDependencyError> {
        if request.cancellation_id.trim().is_empty() {
            return Err(ToolHostDependencyError::InvalidRequest);
        }
        let mut active = None;
        for _ in 0..20 {
            active = self
                .active_tools
                .lock()
                .await
                .get(&request.cancellation_id)
                .cloned();
            if active.is_some() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let Some(active) = active else {
            return Ok(false);
        };
        if !active.cancellation.begin().await {
            return Ok(active.cancellation.wait_for_existing_request().await);
        }
        if active.tool.starts_with("process.") {
            let result = self
                .processes
                .cancel_active(
                    &active.session_id,
                    &active.workspace,
                    &request.cancellation_id,
                )
                .await;
            active
                .cancellation
                .finish(result.as_ref().copied().unwrap_or(false))
                .await;
            return result;
        }
        active.cancellation.finish(false).await;
        Ok(false)
    }

    fn list_receipts(
        &self,
    ) -> Result<Vec<crate::tool::DependencyToolReceipt>, ToolHostDependencyError> {
        self.receipts.list()
    }

    async fn shutdown(&self) {
        self.filesystem.shutdown().await;
        self.browser.shutdown().await;
        self.processes.shutdown().await;
        self.git.shutdown().await;
        self.web.shutdown().await;
        self.lsp.shutdown().await;
        self.mcp.shutdown().await;
    }
}

fn cancelled_tool_events(
    call_id: &str,
    result: Result<Vec<DependencyToolEvent>, ToolHostDependencyError>,
) -> Vec<DependencyToolEvent> {
    let mut events = result
        .unwrap_or_default()
        .into_iter()
        .take_while(|event| {
            !matches!(
                event,
                DependencyToolEvent::Completed { .. }
                    | DependencyToolEvent::Failed { .. }
                    | DependencyToolEvent::Cancelled { .. }
            )
        })
        .collect::<Vec<_>>();
    if !events
        .iter()
        .any(|event| matches!(event, DependencyToolEvent::Started { .. }))
    {
        events.push(DependencyToolEvent::Started {
            call_id: call_id.to_owned(),
        });
    }
    events.push(DependencyToolEvent::Cancelled {
        call_id: call_id.to_owned(),
    });
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_state_distinguishes_idle_failed_and_confirmed_requests() {
        let idle = ActiveToolCancellation::new();
        assert!(!idle.wait_for_existing_request().await);

        let failed = ActiveToolCancellation::new();
        assert!(failed.begin().await);
        failed.finish(false).await;
        assert!(!failed.wait_for_existing_request().await);

        let confirmed = Arc::new(ActiveToolCancellation::new());
        assert!(confirmed.begin().await);
        let waiter = {
            let confirmed = Arc::clone(&confirmed);
            tokio::spawn(async move { confirmed.wait_for_existing_request().await })
        };
        confirmed.finish(true).await;
        assert!(waiter.await.expect("waiter"));
    }

    #[test]
    fn confirmed_cancellation_replaces_successful_terminal_event() {
        let events = cancelled_tool_events(
            "call",
            Ok(vec![
                DependencyToolEvent::Started {
                    call_id: String::from("call"),
                },
                DependencyToolEvent::Completed {
                    call_id: String::from("call"),
                    result: serde_json::json!({"ok": true}),
                    artifact: None,
                    truncated: false,
                },
            ]),
        );
        assert_eq!(
            events,
            vec![
                DependencyToolEvent::Started {
                    call_id: String::from("call"),
                },
                DependencyToolEvent::Cancelled {
                    call_id: String::from("call"),
                },
            ]
        );
    }

    #[test]
    fn confirmed_cancellation_synthesizes_started_after_transport_loss() {
        let events = cancelled_tool_events("call", Err(ToolHostDependencyError::Transport));
        assert_eq!(
            events,
            vec![
                DependencyToolEvent::Started {
                    call_id: String::from("call"),
                },
                DependencyToolEvent::Cancelled {
                    call_id: String::from("call"),
                },
            ]
        );
    }
}
