//! Business-facing MCP datasets and dependency normalization.

use agentmod_mcp_host_dependency::{
    DependencyAuthorization, DependencyInvocationKind, DependencyInvokeRequest, McpDependencyError,
    McpDependencyPort,
};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Data-owned server health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerDataRecord {
    /// Server namespace.
    pub server_id: String,
    /// Active in this host.
    pub active: bool,
    /// Initialized.
    pub initialized: bool,
    /// Transport.
    pub transport: String,
}

/// Data-owned operation descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationDataRecord {
    /// Namespaced identifier exposed to runtime.
    pub namespaced_name: String,
    /// Description.
    pub description: String,
    /// Input schema.
    pub schema: Value,
}

/// Capability dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityDataSet {
    /// Server.
    pub server_id: String,
    /// Protocol.
    pub protocol_version: String,
    /// Tools.
    pub tools: Vec<OperationDataRecord>,
    /// Resource URIs.
    pub resources: Vec<String>,
    /// Prompts.
    pub prompts: Vec<String>,
}

/// Invocation class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationDataKind {
    /// Tool.
    Tool,
    /// Resource.
    Resource,
    /// Prompt.
    Prompt,
}

/// Data invocation request.
#[derive(Clone, Debug, PartialEq)]
pub struct InvokeDataRequest {
    /// Exact data-owned authorization.
    pub authorization: McpDataAuthorization,
    /// Server.
    pub server_id: String,
    /// Kind.
    pub kind: InvocationDataKind,
    /// Unqualified name.
    pub name: String,
    /// Arguments.
    pub arguments: Value,
    /// Cancellation ID.
    pub cancellation_id: String,
}

/// Data-owned authorization record.
#[derive(Clone, Debug, PartialEq)]
pub struct McpDataAuthorization {
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

/// Invocation dataset.
#[derive(Clone, Debug, PartialEq)]
pub struct InvokeDataRecord {
    /// Result.
    pub result: Value,
    /// Progress payloads.
    pub progress: Vec<Value>,
}

/// Data port.
#[async_trait]
pub trait McpDataPort: Send + Sync {
    /// Lists server health.
    async fn list_servers(
        &self,
        authorization: McpDataAuthorization,
    ) -> Result<Vec<ServerDataRecord>, McpDataError>;
    /// Loads capabilities.
    async fn capabilities(
        &self,
        server_id: &str,
        authorization: McpDataAuthorization,
    ) -> Result<CapabilityDataSet, McpDataError>;
    /// Invokes.
    async fn invoke(&self, request: InvokeDataRequest) -> Result<InvokeDataRecord, McpDataError>;
    /// Cancels.
    async fn cancel(&self, cancellation_id: &str) -> Result<(), McpDataError>;
}

/// Data implementation.
#[derive(Clone)]
pub struct McpData<D> {
    dependency: D,
}

impl<D> McpData<D> {
    /// Injects dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: McpDependencyPort> McpDataPort for McpData<D> {
    async fn list_servers(
        &self,
        authorization: McpDataAuthorization,
    ) -> Result<Vec<ServerDataRecord>, McpDataError> {
        let records = self
            .dependency
            .list_servers(map_authorization(authorization))
            .await
            .map_err(map_error)?
            .into_iter()
            .map(|value| ServerDataRecord {
                server_id: value.server_id,
                active: value.active,
                initialized: value.initialized,
                transport: value.transport,
            })
            .collect();
        Ok(records)
    }

    async fn capabilities(
        &self,
        server_id: &str,
        authorization: McpDataAuthorization,
    ) -> Result<CapabilityDataSet, McpDataError> {
        let value = self
            .dependency
            .capabilities(server_id, map_authorization(authorization))
            .await
            .map_err(map_error)?;
        Ok(CapabilityDataSet {
            server_id: value.server_id.clone(),
            protocol_version: value.protocol_version,
            tools: value
                .tools
                .into_iter()
                .map(|tool| OperationDataRecord {
                    namespaced_name: format!("mcp__{}__{}", value.server_id, tool.name),
                    description: tool.description,
                    schema: tool.input_schema,
                })
                .collect(),
            resources: value
                .resources
                .into_iter()
                .map(|record| record.uri)
                .collect(),
            prompts: value
                .prompts
                .into_iter()
                .map(|record| record.name)
                .collect(),
        })
    }

    async fn invoke(&self, request: InvokeDataRequest) -> Result<InvokeDataRecord, McpDataError> {
        let value = self
            .dependency
            .invoke(DependencyInvokeRequest {
                authorization: map_authorization(request.authorization),
                server_id: request.server_id,
                kind: match request.kind {
                    InvocationDataKind::Tool => DependencyInvocationKind::Tool,
                    InvocationDataKind::Resource => DependencyInvocationKind::Resource,
                    InvocationDataKind::Prompt => DependencyInvocationKind::Prompt,
                },
                name: request.name,
                arguments: request.arguments,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(map_error)?;
        Ok(InvokeDataRecord {
            result: value.result,
            progress: value
                .progress
                .into_iter()
                .map(|progress| progress.value)
                .collect(),
        })
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<(), McpDataError> {
        self.dependency
            .cancel(cancellation_id)
            .await
            .map_err(map_error)
    }
}

fn map_authorization(value: McpDataAuthorization) -> DependencyAuthorization {
    DependencyAuthorization {
        call_id: value.call_id,
        action: value.action,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        arguments: value.arguments,
        cancellation_id: value.cancellation_id,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "dependency failures are reduced to data-owned classes"
)]
fn map_error(error: McpDependencyError) -> McpDataError {
    match error {
        McpDependencyError::InvalidRequest => McpDataError::Invalid,
        McpDependencyError::Cancelled => McpDataError::Cancelled,
        McpDependencyError::ServerUnavailable | McpDependencyError::UnknownCancellation => {
            McpDataError::Unavailable
        }
        McpDependencyError::Timeout => McpDataError::Timeout,
        _ => McpDataError::External,
    }
}

/// Data errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum McpDataError {
    /// Invalid request.
    #[error("MCP data request is invalid")]
    Invalid,
    /// Unavailable.
    #[error("MCP data is unavailable")]
    Unavailable,
    /// Timeout.
    #[error("MCP data request timed out")]
    Timeout,
    /// Cancelled.
    #[error("MCP data request was cancelled")]
    Cancelled,
    /// External failure.
    #[error("MCP external operation failed")]
    External,
}
