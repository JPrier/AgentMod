//! Tool-protocol endpoints for MCP server management and invocation.

use agentmod_mcp_host_logic::{
    BeginOAuthCommand, InvocationKind, InvokeCommand, McpAuthorization, McpLogicError,
    McpLogicPort, OAuthStatusKind,
};
use agentmod_tool_protocol::{ToolDescriptor, ToolHostCommand, ToolHostEvent};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

/// MCP service.
#[derive(Clone)]
pub struct McpHostService<L> {
    logic: L,
}

impl<L> McpHostService<L> {
    /// Injects logic.
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerRequest {
    server_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InvokeRequest {
    server_id: String,
    kind: ServiceInvocationKind,
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OAuthCancelRequest {
    server_id: String,
    transaction_id: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ServiceInvocationKind {
    Tool,
    Resource,
    Prompt,
}

#[derive(Clone)]
struct ServiceAuthorization {
    call_id: String,
    action: String,
    normalized_digest: String,
    grant: String,
    arguments: Value,
    cancellation_id: String,
}

impl<L: McpLogicPort> McpHostService<L> {
    /// Handles one tool-protocol command.
    ///
    /// # Errors
    ///
    /// Returns a service-owned error for invalid endpoints or failed business operations.
    #[allow(
        clippy::too_many_lines,
        reason = "the service exhaustively maps each wire operation into service-owned commands"
    )]
    pub async fn handle(
        &self,
        command: ToolHostCommand,
    ) -> Result<Vec<ToolHostEvent>, McpServiceError> {
        match command {
            ToolHostCommand::DiscoverGroups => Ok(vec![ToolHostEvent::Groups {
                groups: vec!["mcp".to_owned()],
            }]),
            ToolHostCommand::DiscoverTools { groups } => Ok(vec![ToolHostEvent::Tools {
                tools: if groups.iter().any(|group| group == "mcp") {
                    descriptors()
                } else {
                    Vec::new()
                },
            }]),
            ToolHostCommand::Health => Ok(vec![ToolHostEvent::Progress {
                call_id: "health".to_owned(),
                message: "MCP host ready".to_owned(),
                completed: Some(1),
                total: Some(1),
            }]),
            ToolHostCommand::Cancel { cancellation_id } => {
                self.logic
                    .cancel(&cancellation_id.to_string())
                    .await
                    .map_err(map_logic_error)?;
                Ok(vec![ToolHostEvent::Cancelled {
                    call_id: cancellation_id.to_string(),
                }])
            }
            ToolHostCommand::Execute {
                call_id,
                tool,
                arguments,
                normalized_digest,
                authorization_grant,
                cancellation_id,
            } => {
                if normalized_digest.len() != 64 || authorization_grant.trim().is_empty() {
                    return Err(McpServiceError::InvalidAuthorization);
                }
                let cancellation_id = cancellation_id.to_string();
                let arguments = canonical_arguments(&tool, arguments)?;
                let authorization = ServiceAuthorization {
                    call_id: call_id.clone(),
                    action: tool.clone(),
                    normalized_digest,
                    grant: authorization_grant,
                    arguments: arguments.clone(),
                    cancellation_id: cancellation_id.clone(),
                };
                match tool.as_str() {
                    "mcp.server.list" => {
                        let servers = self
                            .logic
                            .list_servers(to_logic_authorization(authorization))
                            .await
                            .map_err(map_logic_error)?;
                        Ok(completed(
                            call_id,
                            json!({"servers": servers.into_iter().map(|server| json!({
                                "id":server.id,
                                "active":server.active,
                                "initialized":server.initialized,
                                "transport":server.transport,
                            })).collect::<Vec<_>>() }),
                        ))
                    }
                    "mcp.capabilities" => {
                        let request: ServerRequest =
                            serde_json::from_value(arguments).map_err(invalid)?;
                        let value = self
                            .logic
                            .capabilities(&request.server_id, to_logic_authorization(authorization))
                            .await
                            .map_err(map_logic_error)?;
                        Ok(completed(
                            call_id,
                            json!({
                                "server_id":value.server_id,
                                "protocol_version":value.protocol_version,
                                "tools":value.tools.into_iter().map(|tool|json!({
                                    "name":tool.name,
                                    "description":tool.description,
                                    "input_schema":tool.schema,
                                })).collect::<Vec<_>>(),
                                "resources":value.resources,
                                "prompts":value.prompts,
                            }),
                        ))
                    }
                    "mcp.invoke" => {
                        let request: InvokeRequest =
                            serde_json::from_value(arguments).map_err(invalid)?;
                        self.invoke(call_id, request, authorization).await
                    }
                    "mcp.oauth.begin" => {
                        let request: ServerRequest =
                            serde_json::from_value(arguments).map_err(invalid)?;
                        let cancellation_id = authorization.cancellation_id.clone();
                        let value = self
                            .logic
                            .begin_oauth(BeginOAuthCommand {
                                authorization: to_logic_authorization(authorization),
                                server_id: request.server_id,
                                cancellation_id,
                            })
                            .await
                            .map_err(map_logic_error)?;
                        Ok(completed(
                            call_id,
                            json!({
                                "server_id": value.server_id,
                                "transaction_id": value.transaction_id,
                                "authorization_url": value.authorization_url,
                                "expires_at_ms": value.expires_at_ms,
                                "configuration_hash": value.configuration_hash,
                            }),
                        ))
                    }
                    "mcp.oauth.status" => {
                        let request: ServerRequest =
                            serde_json::from_value(arguments).map_err(invalid)?;
                        let value = self
                            .logic
                            .oauth_status(&request.server_id, to_logic_authorization(authorization))
                            .await
                            .map_err(map_logic_error)?;
                        Ok(completed(call_id, oauth_status_json(&value)))
                    }
                    "mcp.oauth.cancel" => {
                        let request: OAuthCancelRequest =
                            serde_json::from_value(arguments).map_err(invalid)?;
                        let value = self
                            .logic
                            .cancel_oauth(
                                &request.server_id,
                                &request.transaction_id,
                                to_logic_authorization(authorization),
                            )
                            .await
                            .map_err(map_logic_error)?;
                        Ok(completed(call_id, oauth_status_json(&value)))
                    }
                    namespaced if namespaced.starts_with("mcp__") => {
                        let (server_id, name) = parse_namespaced(namespaced)?;
                        self.invoke(
                            call_id,
                            InvokeRequest {
                                server_id,
                                kind: ServiceInvocationKind::Tool,
                                name,
                                arguments,
                            },
                            authorization,
                        )
                        .await
                    }
                    _ => Err(McpServiceError::UnknownTool),
                }
            }
        }
    }

    async fn invoke(
        &self,
        call_id: String,
        request: InvokeRequest,
        authorization: ServiceAuthorization,
    ) -> Result<Vec<ToolHostEvent>, McpServiceError> {
        let cancellation_id = authorization.cancellation_id.clone();
        let value = match self
            .logic
            .invoke(InvokeCommand {
                authorization: to_logic_authorization(authorization),
                server_id: request.server_id,
                kind: match request.kind {
                    ServiceInvocationKind::Tool => InvocationKind::Tool,
                    ServiceInvocationKind::Resource => InvocationKind::Resource,
                    ServiceInvocationKind::Prompt => InvocationKind::Prompt,
                },
                name: request.name,
                arguments: request.arguments,
                cancellation_id,
            })
            .await
        {
            Ok(value) => value,
            Err(McpLogicError::Cancelled) => {
                return Ok(vec![
                    ToolHostEvent::Started {
                        call_id: call_id.clone(),
                    },
                    ToolHostEvent::Cancelled { call_id },
                ]);
            }
            Err(error) => return Err(map_logic_error(error)),
        };
        let mut events = vec![ToolHostEvent::Started {
            call_id: call_id.clone(),
        }];
        events.extend(
            value
                .progress
                .into_iter()
                .enumerate()
                .map(|(index, progress)| ToolHostEvent::Progress {
                    call_id: call_id.clone(),
                    message: progress.to_string(),
                    completed: u64::try_from(index).ok(),
                    total: None,
                }),
        );
        events.push(ToolHostEvent::Completed {
            call_id,
            result: value.result,
            artifact: None,
            truncated: false,
        });
        Ok(events)
    }
}

fn to_logic_authorization(value: ServiceAuthorization) -> McpAuthorization {
    McpAuthorization {
        call_id: value.call_id,
        action: value.action,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        arguments: value.arguments,
        cancellation_id: value.cancellation_id,
    }
}

fn canonical_arguments(tool: &str, arguments: Value) -> Result<Value, McpServiceError> {
    match tool {
        "mcp.server.list" => {
            let object = arguments
                .as_object()
                .ok_or(McpServiceError::InvalidArguments)?;
            if object.is_empty() {
                Ok(json!({}))
            } else {
                Err(McpServiceError::InvalidArguments)
            }
        }
        "mcp.capabilities" | "mcp.oauth.begin" | "mcp.oauth.status" => {
            let request: ServerRequest = serde_json::from_value(arguments).map_err(invalid)?;
            Ok(json!({"server_id":request.server_id}))
        }
        "mcp.invoke" => {
            let request: InvokeRequest = serde_json::from_value(arguments).map_err(invalid)?;
            Ok(json!({
                "server_id":request.server_id,
                "kind":match request.kind {
                    ServiceInvocationKind::Tool => "tool",
                    ServiceInvocationKind::Resource => "resource",
                    ServiceInvocationKind::Prompt => "prompt",
                },
                "name":request.name,
                "arguments":request.arguments,
            }))
        }
        "mcp.oauth.cancel" => {
            let request: OAuthCancelRequest = serde_json::from_value(arguments).map_err(invalid)?;
            Ok(json!({
                "server_id":request.server_id,
                "transaction_id":request.transaction_id,
            }))
        }
        namespaced if namespaced.starts_with("mcp__") && arguments.is_object() => Ok(arguments),
        _ => Err(McpServiceError::UnknownTool),
    }
}

fn oauth_status_json(value: &agentmod_mcp_host_logic::OAuthStatusResult) -> Value {
    json!({
        "server_id": value.server_id,
        "status": match value.status {
            OAuthStatusKind::Unauthorized => "unauthorized",
            OAuthStatusKind::Pending => "pending",
            OAuthStatusKind::Authorized => "authorized",
            OAuthStatusKind::Failed => "failed",
        },
        "transaction_id": value.transaction_id,
        "expires_at_ms": value.expires_at_ms,
        "scopes": value.scopes,
        "configuration_hash": value.configuration_hash,
    })
}

fn parse_namespaced(value: &str) -> Result<(String, String), McpServiceError> {
    let mut components = value.splitn(3, "__");
    if components.next() != Some("mcp") {
        return Err(McpServiceError::UnknownTool);
    }
    let server = components.next().unwrap_or_default();
    let name = components.next().unwrap_or_default();
    if server.is_empty() || name.is_empty() {
        return Err(McpServiceError::InvalidArguments);
    }
    Ok((server.to_owned(), name.to_owned()))
}

fn completed(call_id: String, result: Value) -> Vec<ToolHostEvent> {
    vec![
        ToolHostEvent::Started {
            call_id: call_id.clone(),
        },
        ToolHostEvent::Completed {
            call_id,
            result,
            artifact: None,
            truncated: false,
        },
    ]
}

fn descriptors() -> Vec<ToolDescriptor> {
    vec![
        descriptor(
            "mcp.server.list",
            "List configured MCP servers without starting dormant servers",
            json!({"type":"object","additionalProperties":false}),
        ),
        descriptor(
            "mcp.capabilities",
            "Initialize one MCP server and discover tools, resources, and prompts",
            json!({"type":"object","required":["server_id"],"properties":{"server_id":{"type":"string"}},"additionalProperties":false}),
        ),
        descriptor(
            "mcp.invoke",
            "Invoke an MCP tool, resource, or prompt through the normalized host",
            json!({"type":"object","required":["server_id","kind","name"],"properties":{
                "server_id":{"type":"string"},
                "kind":{"enum":["tool","resource","prompt"]},
                "name":{"type":"string"},
                "arguments":{}
            },"additionalProperties":false}),
        ),
    ]
}

fn descriptor(id: &str, description: &str, input_schema: Value) -> ToolDescriptor {
    ToolDescriptor {
        id: id.to_owned(),
        group: "mcp".to_owned(),
        description: description.to_owned(),
        input_schema,
        supported_decisions: vec![
            "continue".to_owned(),
            "replace".to_owned(),
            "reject".to_owned(),
            "require_approval".to_owned(),
            "defer".to_owned(),
            "cancel".to_owned(),
        ],
    }
}

fn invalid<T>(_error: T) -> McpServiceError {
    McpServiceError::InvalidArguments
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "logic errors are reduced to stable endpoint classes"
)]
fn map_logic_error(error: McpLogicError) -> McpServiceError {
    match error {
        McpLogicError::InvalidCommand => McpServiceError::InvalidArguments,
        McpLogicError::Cancelled => McpServiceError::Cancelled,
        _ => McpServiceError::Logic,
    }
}

/// Service errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum McpServiceError {
    /// Invalid authorization envelope.
    #[error("MCP authorization envelope is invalid")]
    InvalidAuthorization,
    /// Unknown tool.
    #[error("unknown MCP endpoint")]
    UnknownTool,
    /// Invalid arguments.
    #[error("MCP endpoint arguments are invalid")]
    InvalidArguments,
    /// Cancelled.
    #[error("MCP operation was cancelled")]
    Cancelled,
    /// Logic failed.
    #[error("MCP operation failed")]
    Logic,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_management_is_absent_from_model_discovery() {
        let discovered = descriptors();
        assert_eq!(discovered.len(), 3);
        assert!(
            discovered
                .iter()
                .all(|descriptor| !descriptor.id.starts_with("mcp.oauth."))
        );
    }
}
