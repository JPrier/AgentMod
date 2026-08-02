//! Business-facing MCP datasets and dependency normalization.

use agentmod_mcp_host_dependency::{
    DependencyAuthorization, DependencyInvocationKind, DependencyInvokeRequest,
    DependencyOAuthStatusKind, McpDependencyError, McpDependencyPort,
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

/// Data-owned OAuth status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OAuthDataStatusKind {
    /// No authorization.
    Unauthorized,
    /// Authorization awaits callback completion.
    Pending,
    /// Bearer credential is available by reference.
    Authorized,
    /// Prior authorization failed closed.
    Failed,
}

/// Data-owned OAuth start request.
#[derive(Clone, Debug, PartialEq)]
pub struct BeginOAuthDataRequest {
    /// Exact signed runtime authorization.
    pub authorization: McpDataAuthorization,
    /// Exact server.
    pub server_id: String,
    /// Cancellation identity.
    pub cancellation_id: String,
}

/// Data-owned OAuth start record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OAuthStartDataRecord {
    /// Exact server.
    pub server_id: String,
    /// Opaque transaction.
    pub transaction_id: String,
    /// User authorization URL.
    pub authorization_url: String,
    /// Transaction expiry.
    pub expires_at_ms: i64,
    /// Stable hash of the exact configured OAuth server binding.
    pub configuration_hash: String,
}

/// Redacted data-owned OAuth status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OAuthStatusDataRecord {
    /// Exact server.
    pub server_id: String,
    /// Stable status.
    pub status: OAuthDataStatusKind,
    /// Opaque pending transaction.
    pub transaction_id: Option<String>,
    /// Pending transaction or access-token expiry.
    pub expires_at_ms: Option<i64>,
    /// Granted non-secret scopes.
    pub scopes: Vec<String>,
    /// Stable hash of the exact configured OAuth server binding.
    pub configuration_hash: String,
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
    /// Begins an OAuth authorization transaction.
    async fn begin_oauth(
        &self,
        request: BeginOAuthDataRequest,
    ) -> Result<OAuthStartDataRecord, McpDataError>;
    /// Reads redacted OAuth state.
    async fn oauth_status(
        &self,
        server_id: &str,
        authorization: McpDataAuthorization,
    ) -> Result<OAuthStatusDataRecord, McpDataError>;
    /// Cancels an exact pending OAuth transaction.
    async fn cancel_oauth(
        &self,
        server_id: &str,
        transaction_id: &str,
        authorization: McpDataAuthorization,
    ) -> Result<OAuthStatusDataRecord, McpDataError>;
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

    async fn begin_oauth(
        &self,
        request: BeginOAuthDataRequest,
    ) -> Result<OAuthStartDataRecord, McpDataError> {
        let value = self
            .dependency
            .begin_oauth(
                &request.server_id,
                &request.cancellation_id,
                map_authorization(request.authorization),
            )
            .await
            .map_err(map_error)?;
        Ok(OAuthStartDataRecord {
            server_id: value.server_id,
            transaction_id: value.transaction_id,
            authorization_url: value.authorization_url,
            expires_at_ms: value.expires_at_ms,
            configuration_hash: value.configuration_hash,
        })
    }

    async fn oauth_status(
        &self,
        server_id: &str,
        authorization: McpDataAuthorization,
    ) -> Result<OAuthStatusDataRecord, McpDataError> {
        self.dependency
            .oauth_status(server_id, map_authorization(authorization))
            .await
            .map(map_oauth_status)
            .map_err(map_error)
    }

    async fn cancel_oauth(
        &self,
        server_id: &str,
        transaction_id: &str,
        authorization: McpDataAuthorization,
    ) -> Result<OAuthStatusDataRecord, McpDataError> {
        self.dependency
            .cancel_oauth(server_id, transaction_id, map_authorization(authorization))
            .await
            .map(map_oauth_status)
            .map_err(map_error)
    }
}

fn map_oauth_status(
    value: agentmod_mcp_host_dependency::DependencyOAuthStatus,
) -> OAuthStatusDataRecord {
    OAuthStatusDataRecord {
        server_id: value.server_id,
        status: match value.status {
            DependencyOAuthStatusKind::Unauthorized => OAuthDataStatusKind::Unauthorized,
            DependencyOAuthStatusKind::Pending => OAuthDataStatusKind::Pending,
            DependencyOAuthStatusKind::Authorized => OAuthDataStatusKind::Authorized,
            DependencyOAuthStatusKind::Failed => OAuthDataStatusKind::Failed,
        },
        transaction_id: value.transaction_id,
        expires_at_ms: value.expires_at_ms,
        scopes: value.scopes,
        configuration_hash: value.configuration_hash,
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
        McpDependencyError::InvalidRequest | McpDependencyError::OAuthTransaction => {
            McpDataError::Invalid
        }
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
