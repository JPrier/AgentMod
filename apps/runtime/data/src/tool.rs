//! Business-facing tool-host datasets.
#![allow(
    missing_docs,
    reason = "data-local tool records are intentionally boundary-specific"
)]

use std::path::PathBuf;

use agentmod_primitives::ContentHash;
use agentmod_runtime_dependency::tool as dependency;
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Immutable first-party tool identity and provider-alias declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalToolDataRecord {
    pub id: &'static str,
    pub group: &'static str,
    pub aliases: &'static [&'static str],
}

const fn canonical_tool(
    id: &'static str,
    group: &'static str,
    aliases: &'static [&'static str],
) -> CanonicalToolDataRecord {
    CanonicalToolDataRecord { id, group, aliases }
}

const CANONICAL_TOOL_CATALOG: &[CanonicalToolDataRecord] = &[
    canonical_tool("filesystem.read", "filesystem", &["read_file"]),
    canonical_tool("filesystem.list", "filesystem", &["list_files"]),
    canonical_tool("filesystem.glob", "filesystem", &["glob"]),
    canonical_tool("filesystem.grep", "filesystem", &["grep"]),
    canonical_tool("filesystem.write", "filesystem", &["write_file"]),
    canonical_tool("filesystem.edit", "filesystem", &["edit_file"]),
    canonical_tool("filesystem.apply_patch", "filesystem", &["apply_patch"]),
    canonical_tool("process.run", "process", &["run_command"]),
    canonical_tool("process.start", "process", &["start_process"]),
    canonical_tool("process.run_pty", "process", &[]),
    canonical_tool("process.start_pty", "process", &[]),
    canonical_tool("process.read", "process", &[]),
    canonical_tool("process.input", "process", &[]),
    canonical_tool("process.resize", "process", &[]),
    canonical_tool("process.wait", "process", &[]),
    canonical_tool("process.interrupt", "process", &[]),
    canonical_tool("process.kill", "process", &[]),
    canonical_tool("process.detach", "process", &[]),
    canonical_tool("process.reattach", "process", &[]),
    canonical_tool("process.list", "process", &[]),
    canonical_tool("git.discover", "git", &[]),
    canonical_tool("git.status", "git", &[]),
    canonical_tool("git.diff", "git", &[]),
    canonical_tool("git.changed_files", "git", &[]),
    canonical_tool("git.branch", "git", &[]),
    canonical_tool("git.dirty", "git", &[]),
    canonical_tool("git.worktree_create", "git", &[]),
    canonical_tool("git.worktree_cleanup", "git", &[]),
    canonical_tool("git.checkpoint_create", "git", &[]),
    canonical_tool("git.checkpoint_restore", "git", &[]),
    canonical_tool("git.export_patch", "git", &[]),
    canonical_tool("http.request", "web", &[]),
    canonical_tool("web.fetch", "web", &[]),
    canonical_tool("web.search", "web", &[]),
    canonical_tool("lsp.project_root", "lsp", &[]),
    canonical_tool("lsp.diagnostics", "lsp", &[]),
    canonical_tool("lsp.document_symbols", "lsp", &[]),
    canonical_tool("lsp.workspace_symbols", "lsp", &[]),
    canonical_tool("lsp.definition", "lsp", &[]),
    canonical_tool("lsp.references", "lsp", &[]),
    canonical_tool("lsp.hover", "lsp", &[]),
    canonical_tool("lsp.signature_help", "lsp", &[]),
    canonical_tool("lsp.rename", "lsp", &[]),
    canonical_tool("lsp.formatting", "lsp", &[]),
    canonical_tool("lsp.code_actions", "lsp", &[]),
    canonical_tool("mcp.server.list", "mcp", &[]),
    canonical_tool("mcp.capabilities", "mcp", &[]),
    canonical_tool("mcp.invoke", "mcp", &[]),
    canonical_tool("browser.start", "browser", &[]),
    canonical_tool("browser.navigate", "browser", &[]),
    canonical_tool("browser.inspect", "browser", &[]),
    canonical_tool("browser.screenshot", "browser", &[]),
    canonical_tool("browser.click", "browser", &[]),
    canonical_tool("browser.type", "browser", &[]),
    canonical_tool("browser.submit", "browser", &[]),
    canonical_tool("browser.download", "browser", &[]),
    canonical_tool("browser.close", "browser", &[]),
];

/// Returns the one data-owned immutable first-party tool catalog.
#[must_use]
pub const fn canonical_tool_catalog() -> &'static [CanonicalToolDataRecord] {
    CANONICAL_TOOL_CATALOG
}

/// Hashes the exact canonical tool IDs, groups, aliases, and ordering.
#[must_use]
pub fn canonical_tool_catalog_hash() -> ContentHash {
    let mut bytes = b"agentmod.runtime.tool-catalog@1\0".to_vec();
    for tool in CANONICAL_TOOL_CATALOG {
        bytes.extend_from_slice(tool.id.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(tool.group.as_bytes());
        bytes.push(0);
        for alias in tool.aliases {
            bytes.extend_from_slice(alias.as_bytes());
            bytes.push(0);
        }
        bytes.push(0xff);
    }
    ContentHash::digest(&bytes)
}

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
    /// Exact workspace lease authorization selected by runtime logic.
    pub workspace_authorization: Option<WorkspaceAuthorizationDataRecord>,
}

/// Data-owned workspace authorization for one exact tool dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceAuthorizationDataRecord {
    pub lease_id: String,
    pub lease_hash: ContentHash,
    pub read_only: bool,
    pub dispatch_digest: ContentHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelToolDataRequest {
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
    pub workspace_authorization: Option<WorkspaceAuthorizationDataRecord>,
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

    async fn cancel_tool(&self, _request: CancelToolDataRequest) -> Result<bool, ToolDataError> {
        Ok(false)
    }

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
                workspace_authorization: request.workspace_authorization.map(|authorization| {
                    dependency::DependencyWorkspaceAuthorization {
                        lease_id: authorization.lease_id,
                        lease_hash: authorization.lease_hash,
                        read_only: authorization.read_only,
                        dispatch_digest: authorization.dispatch_digest,
                    }
                }),
            })
            .await
            .map_err(|error| map_dependency_error(&error))?
            .into_iter()
            .map(map_event)
            .collect();
        Ok(events)
    }

    async fn cancel_tool(&self, request: CancelToolDataRequest) -> Result<bool, ToolDataError> {
        self.dependency
            .cancel(dependency::DependencyCancelToolRequest {
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|_| ToolDataError::Unavailable)
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
                workspace_authorization: receipt.command.workspace_authorization.map(
                    |authorization| WorkspaceAuthorizationDataRecord {
                        lease_id: authorization.lease_id,
                        lease_hash: authorization.lease_hash,
                        read_only: authorization.read_only,
                        dispatch_digest: authorization.dispatch_digest,
                    },
                ),
            })
            .collect())
    }
}

fn map_dependency_error(error: &dependency::ToolHostDependencyError) -> ToolDataError {
    match error {
        dependency::ToolHostDependencyError::InvalidConfiguration => {
            ToolDataError::InvalidConfiguration
        }
        dependency::ToolHostDependencyError::ReceiptMissing => ToolDataError::ReceiptUnavailable,
        _ => ToolDataError::Unavailable,
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
    #[error("tool-host immutable configuration is invalid")]
    InvalidConfiguration,
    #[error("tool-host data is unavailable")]
    Unavailable,
    #[error("tool-host terminal receipt is unavailable")]
    ReceiptUnavailable,
}

#[cfg(test)]
mod tests {
    use agentmod_runtime_dependency::tool::ToolHostDependencyError;

    use super::{ToolDataError, map_dependency_error};

    #[test]
    fn immutable_configuration_failure_is_not_collapsed_into_host_unavailability() {
        assert_eq!(
            map_dependency_error(&ToolHostDependencyError::InvalidConfiguration),
            ToolDataError::InvalidConfiguration
        );
        assert_eq!(
            map_dependency_error(&ToolHostDependencyError::Unavailable),
            ToolDataError::Unavailable
        );
        assert_eq!(
            map_dependency_error(&ToolHostDependencyError::ReceiptMissing),
            ToolDataError::ReceiptUnavailable
        );
    }
}
