//! Dedicated authenticated MCP OAuth management endpoint.

use agentmod_runtime_logic::mcp_oauth::{
    ManageMcpOAuthCommand, McpOAuthAction, McpOAuthLogicPort, McpOAuthResult,
};
use agentmod_runtime_protocol::{RuntimeRequest, RuntimeResponse};
use async_trait::async_trait;

use crate::local_rpc::{RuntimeEndpointStream, RuntimeWireEndpoint};

/// Service wrapper that keeps OAuth management outside ordinary model tool dispatch.
#[derive(Clone)]
pub struct McpOAuthEndpoint<E, L> {
    inner: E,
    logic: L,
}

impl<E, L> McpOAuthEndpoint<E, L> {
    /// Wraps the normal runtime endpoint with the explicit management route.
    #[must_use]
    pub const fn new(inner: E, logic: L) -> Self {
        Self { inner, logic }
    }
}

impl<E, L> McpOAuthEndpoint<E, L>
where
    E: RuntimeWireEndpoint,
    L: McpOAuthLogicPort,
{
    async fn handle_oauth(
        &self,
        request: &RuntimeRequest,
    ) -> Option<Result<RuntimeResponse, String>> {
        let (session_id, server_id, action, cancellation_id) = match request {
            RuntimeRequest::McpOAuthBegin {
                session_id,
                server_id,
                cancellation_id,
            } => (
                *session_id,
                server_id.clone(),
                McpOAuthAction::Begin,
                *cancellation_id,
            ),
            RuntimeRequest::McpOAuthStatus {
                session_id,
                server_id,
                cancellation_id,
            } => (
                *session_id,
                server_id.clone(),
                McpOAuthAction::Status,
                *cancellation_id,
            ),
            RuntimeRequest::McpOAuthCancel {
                session_id,
                server_id,
                transaction_id,
                cancellation_id,
            } => (
                *session_id,
                server_id.clone(),
                McpOAuthAction::Cancel {
                    transaction_id: transaction_id.clone(),
                },
                *cancellation_id,
            ),
            _ => return None,
        };
        let validated = self
            .inner
            .handle_runtime_request(&RuntimeRequest::InspectSession {
                session_id,
                at: None,
            })
            .await;
        if !matches!(
            validated,
            Ok(RuntimeResponse::SessionInspected {
                session_id: inspected,
                ..
            }) if inspected == session_id
        ) {
            return Some(Err("MCP OAuth session validation failed".into()));
        }
        Some(
            self.logic
                .manage_mcp_oauth(ManageMcpOAuthCommand {
                    session_id,
                    server_id,
                    action,
                    cancellation_id,
                })
                .await
                .map(|result| match result {
                    McpOAuthResult::Begin(value) => RuntimeResponse::McpOAuthStarted {
                        server_id: value.server_id,
                        transaction_id: value.transaction_id,
                        authorization_url: value.authorization_url,
                        authorization_url_hash: value.authorization_url_hash.to_hex(),
                        expires_at_ms: value.expires_at_ms,
                    },
                    McpOAuthResult::Status(value) => RuntimeResponse::McpOAuthStatus {
                        server_id: value.server_id,
                        status: value.status,
                        transaction_id: value.transaction_id,
                        expires_at_ms: value.expires_at_ms,
                        scopes: value.scopes,
                        status_hash: value.status_hash.to_hex(),
                    },
                })
                .map_err(|error| error.to_string()),
        )
    }
}

#[async_trait]
impl<E, L> RuntimeWireEndpoint for McpOAuthEndpoint<E, L>
where
    E: RuntimeWireEndpoint + Send + Sync,
    L: McpOAuthLogicPort + Send + Sync,
{
    async fn handle_runtime_request(
        &self,
        request: &RuntimeRequest,
    ) -> Result<RuntimeResponse, String> {
        if let Some(result) = self.handle_oauth(request).await {
            result
        } else {
            self.inner.handle_runtime_request(request).await
        }
    }

    async fn handle_runtime_stream(
        &self,
        request: &RuntimeRequest,
    ) -> Result<RuntimeEndpointStream, String> {
        if let Some(result) = self.handle_oauth(request).await {
            result.map(RuntimeEndpointStream::single)
        } else {
            self.inner.handle_runtime_stream(request).await
        }
    }
}
