//! Business-facing tool-host datasets.
#![allow(
    missing_docs,
    reason = "data-local tool records are intentionally boundary-specific"
)]

use std::path::PathBuf;

use agentmod_runtime_dependency::tool as dependency;
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct ExecuteToolDataRequest {
    pub execution_id: String,
    pub receipt_only: bool,
    pub session_id: String,
    pub workspace: PathBuf,
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecoverableToolDataRecord {
    pub execution_id: String,
    pub session_id: String,
    pub workspace: PathBuf,
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ToolDataEvent {
    Started {
        call_id: String,
    },
    Progress {
        call_id: String,
        message: String,
        completed: Option<u64>,
        total: Option<u64>,
    },
    Output {
        call_id: String,
        stream: ToolDataOutputStream,
        content: String,
    },
    Completed {
        call_id: String,
        result: Value,
        artifact: Option<String>,
        truncated: bool,
    },
    Failed {
        call_id: String,
        code: String,
        message: String,
        retryable: bool,
    },
    Cancelled {
        call_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolDataOutputStream {
    Standard,
    Error,
}

#[async_trait]
pub trait ToolDataPort: Send + Sync {
    async fn execute_tool(
        &self,
        request: ExecuteToolDataRequest,
    ) -> Result<Vec<ToolDataEvent>, ToolDataError>;

    /// Returns dependency-normalized terminal receipts for startup recovery.
    ///
    /// # Errors
    ///
    /// Returns a data-owned error when the selected receipt dependency is
    /// unavailable or reports invalid storage.
    fn list_tool_receipts(&self) -> Result<Vec<RecoverableToolDataRecord>, ToolDataError> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl<D: dependency::ToolHostDependencyPort> ToolDataPort for super::RuntimeData<D> {
    async fn execute_tool(
        &self,
        request: ExecuteToolDataRequest,
    ) -> Result<Vec<ToolDataEvent>, ToolDataError> {
        let events = self
            .dependency
            .execute(dependency::DependencyToolCommand {
                execution_id: request.execution_id,
                receipt_only: request.receipt_only,
                session_id: request.session_id,
                workspace: request.workspace,
                call_id: request.call_id,
                tool: request.tool,
                arguments: request.arguments,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| match error {
                dependency::ToolHostDependencyError::ReceiptMissing => {
                    ToolDataError::ReceiptUnavailable
                }
                _ => ToolDataError::Unavailable,
            })?
            .into_iter()
            .map(map_event)
            .collect();
        Ok(events)
    }

    fn list_tool_receipts(&self) -> Result<Vec<RecoverableToolDataRecord>, ToolDataError> {
        Ok(self
            .dependency
            .list_receipts()
            .map_err(|_| ToolDataError::Unavailable)?
            .into_iter()
            .map(|receipt| RecoverableToolDataRecord {
                execution_id: receipt.command.execution_id,
                session_id: receipt.command.session_id,
                workspace: receipt.command.workspace,
                call_id: receipt.command.call_id,
                tool: receipt.command.tool,
                arguments: receipt.command.arguments,
                cancellation_id: receipt.command.cancellation_id,
            })
            .collect())
    }
}

fn map_event(event: dependency::DependencyToolEvent) -> ToolDataEvent {
    match event {
        dependency::DependencyToolEvent::Started { call_id } => ToolDataEvent::Started { call_id },
        dependency::DependencyToolEvent::Progress {
            call_id,
            message,
            completed,
            total,
        } => ToolDataEvent::Progress {
            call_id,
            message,
            completed,
            total,
        },
        dependency::DependencyToolEvent::Output {
            call_id,
            stream,
            content,
        } => ToolDataEvent::Output {
            call_id,
            stream: match stream {
                dependency::DependencyOutputStream::Standard => ToolDataOutputStream::Standard,
                dependency::DependencyOutputStream::Error => ToolDataOutputStream::Error,
            },
            content,
        },
        dependency::DependencyToolEvent::Completed {
            call_id,
            result,
            artifact,
            truncated,
        } => ToolDataEvent::Completed {
            call_id,
            result,
            artifact,
            truncated,
        },
        dependency::DependencyToolEvent::Failed {
            call_id,
            code,
            message,
            retryable,
        } => ToolDataEvent::Failed {
            call_id,
            code,
            message,
            retryable,
        },
        dependency::DependencyToolEvent::Cancelled { call_id } => {
            ToolDataEvent::Cancelled { call_id }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ToolDataError {
    #[error("tool-host data is unavailable")]
    Unavailable,
    #[error("tool-host terminal receipt is unavailable")]
    ReceiptUnavailable,
}
