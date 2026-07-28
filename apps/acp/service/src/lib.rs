//! Official ACP v1 endpoint mapping.
#![allow(
    clippy::missing_errors_doc,
    reason = "the service exposes the official ACP error envelope"
)]

use std::{path::Path, sync::Arc};

use agent_client_protocol::{
    Agent, Stdio,
    schema::{
        ProtocolVersion,
        v1::{
            AgentCapabilities, CancelNotification, ContentBlock, ContentChunk, Implementation,
            InitializeRequest, InitializeResponse, LoadSessionRequest, LoadSessionResponse,
            NewSessionRequest, NewSessionResponse, PermissionOption, PermissionOptionKind,
            PromptRequest, PromptResponse, RequestPermissionOutcome, RequestPermissionRequest,
            SessionNotification, SessionUpdate, StopReason, ToolCall, ToolCallStatus,
            ToolCallUpdate, ToolCallUpdateFields,
        },
    },
};
use agentmod_acp_logic::{
    AcpLogicError, AcpLogicPort, ApprovalRequired, CreateSessionCommand, LoadSessionCommand,
    PromptCommand, PromptPart, PromptStream, PromptStreamItem, PromptUpdate,
};

/// ACP service endpoint assembled around injected logic.
pub struct AcpService<L> {
    logic: Arc<L>,
}

impl<L> AcpService<L> {
    /// Creates the endpoint.
    #[must_use]
    pub fn new(logic: L) -> Self {
        Self {
            logic: Arc::new(logic),
        }
    }
}

impl<L: AcpLogicPort + 'static> AcpService<L> {
    /// Serves official ACP v1 JSON-RPC over stdio until EOF.
    #[allow(
        clippy::too_many_lines,
        reason = "the official typed SDK builder keeps endpoint registration in one composition chain"
    )]
    pub async fn run(self) -> agent_client_protocol::Result<()> {
        let initialize_logic = Arc::clone(&self.logic);
        let new_logic = Arc::clone(&self.logic);
        let load_logic = Arc::clone(&self.logic);
        let prompt_logic = Arc::clone(&self.logic);
        let cancel_logic = Arc::clone(&self.logic);

        Agent
            .builder()
            .name("agentmod-acp")
            .on_receive_request(
                async move |request: InitializeRequest, responder, _connection| {
                    let _ = &initialize_logic;
                    responder.respond(
                        InitializeResponse::new(request.protocol_version)
                            .agent_capabilities(AgentCapabilities::new().load_session(true))
                            .agent_info(Implementation::new(
                                "agentmod",
                                env!("CARGO_PKG_VERSION"),
                            )),
                    )
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: NewSessionRequest, responder, _connection| {
                    if !valid_workspace(&request.cwd)
                        || !request.additional_directories.is_empty()
                        || !request.mcp_servers.is_empty()
                    {
                        return responder.respond_with_error(invalid_params(
                            "cwd must be absolute; additional directories and per-session MCP servers are not yet supported",
                        ));
                    }
                    match new_logic
                        .create_session(CreateSessionCommand {
                            workspace: request.cwd.to_string_lossy().into_owned(),
                        })
                        .await
                    {
                        Ok(session_id) => {
                            responder.respond(NewSessionResponse::new(session_id))
                        }
                        Err(error) => responder.respond_with_error(map_logic(error)),
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: LoadSessionRequest, responder, _connection| {
                    if !valid_workspace(&request.cwd)
                        || !request.additional_directories.is_empty()
                        || !request.mcp_servers.is_empty()
                    {
                        return responder.respond_with_error(invalid_params(
                            "loaded cwd must be absolute and optional roots/servers must be empty",
                        ));
                    }
                    match load_logic
                        .load_session(LoadSessionCommand {
                            session_id: request.session_id.to_string(),
                            workspace: request.cwd.to_string_lossy().into_owned(),
                        })
                        .await
                    {
                        Ok(()) => responder.respond(LoadSessionResponse::new()),
                        Err(error) => responder.respond_with_error(map_logic(error)),
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: PromptRequest, responder, connection| {
                    let session_id = request.session_id.to_string();
                    let parts = match map_prompt(request.prompt) {
                        Ok(parts) => parts,
                        Err(error) => return responder.respond_with_error(error),
                    };
                    let mut stream = match prompt_logic
                        .prompt_stream(PromptCommand {
                            session_id,
                            parts,
                        })
                        .await
                    {
                        Ok(stream) => stream,
                        Err(error) => return responder.respond_with_error(map_logic(error)),
                    };
                    let logic = Arc::clone(&prompt_logic);
                    let task_connection = connection.clone();
                    connection.spawn(async move {
                        complete_prompt(logic, task_connection, responder, &mut stream).await
                    })?;
                    Ok(())
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_notification(
                async move |request: CancelNotification, _connection| {
                    let _ = cancel_logic
                        .cancel_session(request.session_id.to_string())
                        .await;
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .connect_to(Stdio::new())
            .await
    }
}

async fn complete_prompt<L: AcpLogicPort + 'static>(
    logic: Arc<L>,
    connection: agent_client_protocol::ConnectionTo<agent_client_protocol::Client>,
    responder: agent_client_protocol::Responder<PromptResponse>,
    stream: &mut PromptStream,
) -> agent_client_protocol::Result<()> {
    while let Some(item) = stream.recv().await {
        match item.map_err(map_logic)? {
            PromptStreamItem::Update(update) => {
                emit_update(&connection, &stream.session_id, update)?;
            }
            PromptStreamItem::Approval(approval) => {
                let selected = connection
                    .send_request(permission_request(&stream.session_id, &approval))
                    .block_task()
                    .await?;
                let (approved, cancelled) = match selected.outcome {
                    RequestPermissionOutcome::Selected(selected) => {
                        (selected.option_id.to_string() == "allow-once", false)
                    }
                    _ => (false, true),
                };
                let updates = logic
                    .resolve_approval(stream.session_id.clone(), approval, approved)
                    .await
                    .map_err(map_logic)?;
                if cancelled {
                    return responder.respond(PromptResponse::new(StopReason::Cancelled));
                }
                emit_updates(&connection, &stream.session_id, updates)?;
                return responder.respond(PromptResponse::new(StopReason::EndTurn));
            }
            PromptStreamItem::Complete => {
                return responder.respond(PromptResponse::new(StopReason::EndTurn));
            }
            PromptStreamItem::Cancelled => {
                return responder.respond(PromptResponse::new(StopReason::Cancelled));
            }
        }
    }
    responder.respond_with_error(map_logic(AcpLogicError::StreamClosed))
}

fn emit_updates(
    connection: &agent_client_protocol::ConnectionTo<agent_client_protocol::Client>,
    session_id: &str,
    updates: Vec<PromptUpdate>,
) -> agent_client_protocol::Result<()> {
    for update in updates {
        emit_update(connection, session_id, update)?;
    }
    Ok(())
}

fn emit_update(
    connection: &agent_client_protocol::ConnectionTo<agent_client_protocol::Client>,
    session_id: &str,
    update: PromptUpdate,
) -> agent_client_protocol::Result<()> {
    let update = match update {
        PromptUpdate::Text(text) => {
            SessionUpdate::AgentMessageChunk(ContentChunk::new(text.into()))
        }
        PromptUpdate::ToolCall {
            call_id,
            name,
            arguments,
        } => SessionUpdate::ToolCall(
            ToolCall::new(call_id, name)
                .status(ToolCallStatus::Pending)
                .raw_input(arguments),
        ),
        PromptUpdate::Failure { code, message } => SessionUpdate::AgentMessageChunk(
            ContentChunk::new(format!("AgentMod error `{code}`: {message}").into()),
        ),
    };
    connection.send_notification(SessionNotification::new(session_id.to_owned(), update))
}

fn permission_request(session_id: &str, approval: &ApprovalRequired) -> RequestPermissionRequest {
    RequestPermissionRequest::new(
        session_id.to_owned(),
        ToolCallUpdate::new(
            approval.call_id.clone(),
            ToolCallUpdateFields::new()
                .title(Some(approval.tool.clone()))
                .status(Some(ToolCallStatus::Pending))
                .raw_input(Some(approval.arguments.clone())),
        ),
        vec![
            PermissionOption::new("allow-once", "Allow once", PermissionOptionKind::AllowOnce),
            PermissionOption::new(
                "reject-once",
                "Reject once",
                PermissionOptionKind::RejectOnce,
            ),
        ],
    )
}

fn map_prompt(content: Vec<ContentBlock>) -> Result<Vec<PromptPart>, agent_client_protocol::Error> {
    content
        .into_iter()
        .map(|content| match content {
            ContentBlock::Text(text) => Ok(PromptPart::Text(text.text)),
            ContentBlock::ResourceLink(link) => Ok(PromptPart::ResourceLink {
                name: link.name,
                uri: link.uri,
            }),
            _ => Err(invalid_params(
                "this AgentMod ACP adapter currently accepts text and resource links",
            )),
        })
        .collect()
}

fn valid_workspace(path: &Path) -> bool {
    path.is_absolute() && !path.as_os_str().is_empty()
}

fn invalid_params(message: &str) -> agent_client_protocol::Error {
    agent_client_protocol::Error::invalid_params().data(message)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "service boundary consumes and translates the logic error"
)]
fn map_logic(error: AcpLogicError) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(error.to_string())
}

/// The stable ACP version implemented by this adapter.
#[must_use]
pub const fn supported_protocol_version() -> ProtocolVersion {
    ProtocolVersion::V1
}
