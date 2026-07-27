//! Concrete dependency bundle for the long-running runtime composition root.

use async_trait::async_trait;

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
        DependencyToolCommand, DependencyToolEvent, ProcessToolHostDependency,
        ToolHostDependencyError, ToolHostDependencyPort,
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
}

impl SupervisedRuntimeDependencies {
    /// Creates a concrete dependency bundle.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root explicitly injects each isolated capability boundary"
    )]
    pub const fn new(
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
        let events = if command.tool.starts_with("browser.") {
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
        }?;
        self.receipts.persist(&command, &events)?;
        if !self.receipts.post_persist_delay().is_zero() {
            tokio::time::sleep(self.receipts.post_persist_delay()).await;
        }
        Ok(events)
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
