//! MCP activation, namespacing, capability, and invocation business logic.

use agentmod_mcp_host_data::{
    InvocationDataKind, InvokeDataRequest, McpDataAuthorization, McpDataError, McpDataPort,
};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Server result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerResult {
    /// ID.
    pub id: String,
    /// Active.
    pub active: bool,
    /// Initialized.
    pub initialized: bool,
    /// Transport.
    pub transport: String,
}

/// Tool result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolResult {
    /// Namespaced tool.
    pub name: String,
    /// Description.
    pub description: String,
    /// Schema.
    pub schema: Value,
}

/// Capabilities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityResult {
    /// Server.
    pub server_id: String,
    /// Protocol.
    pub protocol_version: String,
    /// Tools.
    pub tools: Vec<ToolResult>,
    /// Resources.
    pub resources: Vec<String>,
    /// Prompts.
    pub prompts: Vec<String>,
}

/// Invocation kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationKind {
    /// Tool.
    Tool,
    /// Resource.
    Resource,
    /// Prompt.
    Prompt,
}

/// Invocation command.
#[derive(Clone, Debug, PartialEq)]
pub struct InvokeCommand {
    /// Exact logic-owned authorization.
    pub authorization: McpAuthorization,
    /// Server.
    pub server_id: String,
    /// Kind.
    pub kind: InvocationKind,
    /// Name.
    pub name: String,
    /// Arguments.
    pub arguments: Value,
    /// Cancellation.
    pub cancellation_id: String,
}

/// Logic-owned authorization record.
#[derive(Clone, Debug, PartialEq)]
pub struct McpAuthorization {
    /// Protocol call ID.
    pub call_id: String,
    /// Exact action ID.
    pub action: String,
    /// Runtime-supplied digest.
    pub normalized_digest: String,
    /// Signed grant.
    pub grant: String,
    /// Expanded service arguments.
    pub arguments: Value,
    /// Bound cancellation ID.
    pub cancellation_id: String,
}

/// Invocation result.
#[derive(Clone, Debug, PartialEq)]
pub struct InvokeResult {
    /// Result.
    pub result: Value,
    /// Progress.
    pub progress: Vec<Value>,
}

/// Logic interface.
#[async_trait]
pub trait McpLogicPort: Send + Sync {
    /// Lists servers.
    async fn list_servers(
        &self,
        authorization: McpAuthorization,
    ) -> Result<Vec<ServerResult>, McpLogicError>;
    /// Loads capabilities.
    async fn capabilities(
        &self,
        server_id: &str,
        authorization: McpAuthorization,
    ) -> Result<CapabilityResult, McpLogicError>;
    /// Invokes.
    async fn invoke(&self, command: InvokeCommand) -> Result<InvokeResult, McpLogicError>;
    /// Cancels.
    async fn cancel(&self, cancellation_id: &str) -> Result<(), McpLogicError>;
}

/// Logic.
#[derive(Clone)]
pub struct McpLogic<D> {
    data: D,
}

impl<D> McpLogic<D> {
    /// Injects data.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

#[async_trait]
impl<D: McpDataPort> McpLogicPort for McpLogic<D> {
    async fn list_servers(
        &self,
        authorization: McpAuthorization,
    ) -> Result<Vec<ServerResult>, McpLogicError> {
        validate_authorization(&authorization)?;
        Ok(self
            .data
            .list_servers(map_authorization(authorization))
            .await
            .map_err(map_error)?
            .into_iter()
            .map(|value| ServerResult {
                id: value.server_id,
                active: value.active,
                initialized: value.initialized,
                transport: value.transport,
            })
            .collect())
    }

    async fn capabilities(
        &self,
        server_id: &str,
        authorization: McpAuthorization,
    ) -> Result<CapabilityResult, McpLogicError> {
        validate_component(server_id)?;
        validate_authorization(&authorization)?;
        let value = self
            .data
            .capabilities(server_id, map_authorization(authorization))
            .await
            .map_err(map_error)?;
        Ok(CapabilityResult {
            server_id: value.server_id,
            protocol_version: value.protocol_version,
            tools: value
                .tools
                .into_iter()
                .map(|tool| ToolResult {
                    name: tool.namespaced_name,
                    description: tool.description,
                    schema: tool.schema,
                })
                .collect(),
            resources: value.resources,
            prompts: value.prompts,
        })
    }

    async fn invoke(&self, command: InvokeCommand) -> Result<InvokeResult, McpLogicError> {
        validate_authorization(&command.authorization)?;
        validate_component(&command.server_id)?;
        if command.name.trim().is_empty()
            || command.name.len() > 1024
            || command.cancellation_id.trim().is_empty()
        {
            return Err(McpLogicError::InvalidCommand);
        }
        let value = self
            .data
            .invoke(InvokeDataRequest {
                authorization: map_authorization(command.authorization),
                server_id: command.server_id,
                kind: match command.kind {
                    InvocationKind::Tool => InvocationDataKind::Tool,
                    InvocationKind::Resource => InvocationDataKind::Resource,
                    InvocationKind::Prompt => InvocationDataKind::Prompt,
                },
                name: command.name,
                arguments: command.arguments,
                cancellation_id: command.cancellation_id,
            })
            .await
            .map_err(map_error)?;
        Ok(InvokeResult {
            result: value.result,
            progress: value.progress,
        })
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<(), McpLogicError> {
        if cancellation_id.trim().is_empty() {
            return Err(McpLogicError::InvalidCommand);
        }
        self.data.cancel(cancellation_id).await.map_err(map_error)
    }
}

fn validate_authorization(value: &McpAuthorization) -> Result<(), McpLogicError> {
    if value.call_id.trim().is_empty()
        || value.action.trim().is_empty()
        || value.normalized_digest.len() != 64
        || value.grant.trim().is_empty()
        || !value.arguments.is_object()
        || value.cancellation_id.trim().is_empty()
    {
        Err(McpLogicError::InvalidCommand)
    } else {
        Ok(())
    }
}

fn map_authorization(value: McpAuthorization) -> McpDataAuthorization {
    McpDataAuthorization {
        call_id: value.call_id,
        action: value.action,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        arguments: value.arguments,
        cancellation_id: value.cancellation_id,
    }
}

fn validate_component(value: &str) -> Result<(), McpLogicError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(McpLogicError::InvalidCommand)
    } else {
        Ok(())
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "data failures are reduced to stable business classes"
)]
fn map_error(error: McpDataError) -> McpLogicError {
    match error {
        McpDataError::Invalid => McpLogicError::InvalidCommand,
        McpDataError::Unavailable => McpLogicError::Unavailable,
        McpDataError::Timeout => McpLogicError::Timeout,
        McpDataError::Cancelled => McpLogicError::Cancelled,
        McpDataError::External => McpLogicError::Operation,
    }
}

/// Logic errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum McpLogicError {
    /// Invalid.
    #[error("MCP command is invalid")]
    InvalidCommand,
    /// Unavailable.
    #[error("MCP server is unavailable")]
    Unavailable,
    /// Timeout.
    #[error("MCP operation timed out")]
    Timeout,
    /// Cancelled.
    #[error("MCP operation was cancelled")]
    Cancelled,
    /// Operation failed.
    #[error("MCP operation failed")]
    Operation,
}
