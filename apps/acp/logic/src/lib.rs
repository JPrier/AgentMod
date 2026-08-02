//! ACP session and prompt business logic.
#![allow(missing_docs, reason = "logic-local records are boundary-specific")]
#![allow(
    async_fn_in_trait,
    reason = "the first-party ACP logic boundary intentionally uses static async dispatch"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the logic port exposes one closed error taxonomy"
)]

use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
};

use agentmod_acp_data::{
    AcpDataError, AcpDataPort, CreateSessionDataRequest, SessionMcpSensitiveEntryData,
    SessionMcpServerData, SessionMcpTransportData, TurnDataEvent, TurnDataStream,
    TurnDataStreamItem,
};
use agentmod_primitives::{CancellationId, SessionId};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionCommand {
    pub workspace: String,
    pub mcp_servers: Vec<SessionMcpServer>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadSessionCommand {
    pub session_id: String,
    pub workspace: String,
    pub mcp_servers: Vec<SessionMcpServer>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionMcpServer {
    pub name: String,
    pub transport: SessionMcpTransport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionMcpTransport {
    Stdio {
        command: String,
        arguments: Vec<String>,
        environment: Vec<SessionMcpKeyValue>,
    },
    Http {
        url: String,
        headers: Vec<SessionMcpKeyValue>,
    },
    Sse {
        url: String,
        headers: Vec<SessionMcpKeyValue>,
    },
}

#[derive(Clone, Eq, PartialEq)]
pub struct SessionMcpKeyValue {
    pub name: String,
    pub value: String,
}

impl fmt::Debug for SessionMcpKeyValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionMcpKeyValue")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptPart {
    Text(String),
    ResourceLink {
        name: String,
        uri: String,
    },
    Image {
        data: String,
        mime_type: String,
        uri: Option<String>,
    },
    Audio {
        data: String,
        mime_type: String,
    },
    EmbeddedText {
        text: String,
        uri: String,
        mime_type: Option<String>,
    },
    EmbeddedBlob {
        data: String,
        uri: String,
        mime_type: Option<String>,
    },
}

const MAX_PROMPT_PARTS: usize = 64;
const MAX_PROMPT_BYTES: usize = 1024 * 1024;
const MAX_TEXT_BYTES: usize = 256 * 1024;
const MAX_BINARY_BYTES: usize = 512 * 1024;
const MAX_URI_BYTES: usize = 4096;
const MAX_LABEL_BYTES: usize = 1024;
const MAX_MIME_BYTES: usize = 127;
const MAX_MCP_SERVERS: usize = 32;
const MAX_MCP_NAME_BYTES: usize = 128;
const MAX_MCP_ARGUMENTS: usize = 128;
const MAX_MCP_KEY_VALUES: usize = 64;
const MAX_MCP_FIELD_BYTES: usize = 8192;
const MAX_MCP_DECLARATION_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptCommand {
    pub session_id: String,
    pub parts: Vec<PromptPart>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PromptUpdate {
    Text(String),
    ToolCall {
        call_id: String,
        name: String,
        arguments: Value,
    },
    Failure {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApprovalRequired {
    pub continuation_id: String,
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PromptStreamItem {
    Update(PromptUpdate),
    Approval(ApprovalRequired),
    Complete,
    Cancelled,
}

pub struct PromptStream {
    pub session_id: String,
    receiver: mpsc::Receiver<Result<PromptStreamItem, AcpLogicError>>,
}

impl PromptStream {
    pub async fn recv(&mut self) -> Option<Result<PromptStreamItem, AcpLogicError>> {
        self.receiver.recv().await
    }
}

#[async_trait]
pub trait AcpLogicPort: Send + Sync {
    async fn create_session(&self, command: CreateSessionCommand) -> Result<String, AcpLogicError>;
    async fn load_session(&self, command: LoadSessionCommand) -> Result<(), AcpLogicError>;
    async fn prompt_stream(&self, command: PromptCommand) -> Result<PromptStream, AcpLogicError>;
    async fn resolve_approval(
        &self,
        session_id: String,
        approval: ApprovalRequired,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<Vec<PromptUpdate>, AcpLogicError>;
    async fn cancel_session(&self, session_id: String) -> Result<(), AcpLogicError>;
}

pub struct AcpLogic<D> {
    data: D,
    active: Arc<Mutex<HashMap<SessionId, CancellationId>>>,
}

impl<D> AcpLogic<D> {
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<D: AcpDataPort + Clone + 'static> AcpLogicPort for AcpLogic<D> {
    async fn create_session(&self, command: CreateSessionCommand) -> Result<String, AcpLogicError> {
        if command.workspace.trim().is_empty() {
            return Err(AcpLogicError::InvalidWorkspace);
        }
        validate_mcp_servers(&command.mcp_servers)?;
        self.data
            .create_session(CreateSessionDataRequest {
                workspace: command.workspace,
                style: String::from("persistent-chat"),
                mcp_servers: command
                    .mcp_servers
                    .into_iter()
                    .map(to_data_mcp_server)
                    .collect(),
            })
            .await
            .map(|value| value.to_string())
            .map_err(map_error)
    }

    async fn load_session(&self, command: LoadSessionCommand) -> Result<(), AcpLogicError> {
        validate_mcp_servers(&command.mcp_servers)?;
        let session_id = parse_session(&command.session_id)?;
        let session = self
            .data
            .find_session(session_id)
            .await
            .map_err(map_error)?
            .ok_or(AcpLogicError::SessionNotFound)?;
        if !same_workspace(&session.workspace, &command.workspace) {
            return Err(AcpLogicError::WorkspaceMismatch);
        }
        let expected_mcp_hash = mcp_declaration_hash(&command.mcp_servers)?;
        if session.mcp_declaration_hash.as_deref() != Some(expected_mcp_hash.as_str()) {
            return Err(AcpLogicError::McpBindingMismatch);
        }
        Ok(())
    }

    async fn prompt_stream(&self, command: PromptCommand) -> Result<PromptStream, AcpLogicError> {
        let session_id = parse_session(&command.session_id)?;
        let prompt = render_prompt(command.parts)?;
        let cancellation_id = CancellationId::from_uuid(Uuid::now_v7());
        {
            let mut active = self
                .active
                .lock()
                .map_err(|_| AcpLogicError::StateUnavailable)?;
            if active.contains_key(&session_id) {
                return Err(AcpLogicError::SessionBusy);
            }
            active.insert(session_id, cancellation_id);
        }
        let turn = self
            .data
            .run_turn_stream(session_id, prompt, cancellation_id)
            .await
            .map_err(|error| {
                remove_active(&self.active, session_id, cancellation_id);
                map_error(error)
            })?;
        let receiver = spawn_prompt_forwarder(
            turn,
            self.data.clone(),
            Arc::clone(&self.active),
            session_id,
            cancellation_id,
        );
        Ok(PromptStream {
            session_id: session_id.to_string(),
            receiver,
        })
    }

    async fn resolve_approval(
        &self,
        session_id: String,
        approval: ApprovalRequired,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<Vec<PromptUpdate>, AcpLogicError> {
        let session_id = parse_session(&session_id)?;
        self.data
            .resolve_approval(
                session_id,
                approval.continuation_id,
                approved,
                resume_after_resolution,
            )
            .await
            .map(|events| {
                events
                    .into_iter()
                    .filter_map(|event| match event {
                        TurnDataEvent::Text(value) => Some(PromptUpdate::Text(value)),
                        TurnDataEvent::Failed { code, message, .. } => {
                            Some(PromptUpdate::Failure { code, message })
                        }
                        _ => None,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    async fn cancel_session(&self, session_id: String) -> Result<(), AcpLogicError> {
        let session_id = parse_session(&session_id)?;
        let cancellation_id = self
            .active
            .lock()
            .map_err(|_| AcpLogicError::StateUnavailable)?
            .get(&session_id)
            .copied()
            .ok_or(AcpLogicError::NoActiveTurn)?;
        self.data
            .cancel(cancellation_id, String::from("cancelled by ACP client"))
            .await
            .map_err(map_error)
    }
}

fn spawn_prompt_forwarder<D: AcpDataPort + Clone + 'static>(
    mut turn: TurnDataStream,
    data: D,
    active: Arc<Mutex<HashMap<SessionId, CancellationId>>>,
    session_id: SessionId,
    cancellation_id: CancellationId,
) -> mpsc::Receiver<Result<PromptStreamItem, AcpLogicError>> {
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        forward_prompt_stream(
            &mut turn,
            &data,
            &active,
            session_id,
            cancellation_id,
            &sender,
        )
        .await;
    });
    receiver
}

async fn forward_prompt_stream<D: AcpDataPort>(
    turn: &mut TurnDataStream,
    data: &D,
    active: &Mutex<HashMap<SessionId, CancellationId>>,
    session_id: SessionId,
    cancellation_id: CancellationId,
    sender: &mpsc::Sender<Result<PromptStreamItem, AcpLogicError>>,
) {
    let mut pending_approval = None;
    while let Some(item) = turn.recv().await {
        let item = match item {
            Ok(item) => item,
            Err(error) => {
                let _ = sender.send(Err(map_error(error))).await;
                remove_active(active, session_id, cancellation_id);
                return;
            }
        };
        let outgoing = match item {
            TurnDataStreamItem::Event(TurnDataEvent::Text(value)) => {
                Some(PromptStreamItem::Update(PromptUpdate::Text(value)))
            }
            TurnDataStreamItem::Event(TurnDataEvent::ToolProposed {
                continuation_id,
                call_id,
                tool,
                arguments,
            }) => {
                pending_approval = Some(ApprovalRequired {
                    continuation_id,
                    call_id: call_id.clone(),
                    tool: tool.clone(),
                    arguments: arguments.clone(),
                });
                Some(PromptStreamItem::Update(PromptUpdate::ToolCall {
                    call_id,
                    name: tool,
                    arguments,
                }))
            }
            TurnDataStreamItem::Event(TurnDataEvent::Cancelled) => {
                remove_active(active, session_id, cancellation_id);
                if sender.send(Ok(PromptStreamItem::Cancelled)).await.is_err() {
                    cancel_disconnected(data, cancellation_id).await;
                }
                return;
            }
            TurnDataStreamItem::Event(TurnDataEvent::Failed { code, message, .. }) => {
                Some(PromptStreamItem::Update(PromptUpdate::Failure {
                    code,
                    message,
                }))
            }
            TurnDataStreamItem::Complete {
                awaiting_continuation,
            } => {
                let terminal = match awaiting_continuation {
                    Some(continuation_id) => match pending_approval {
                        Some(mut approval) => {
                            approval.continuation_id = continuation_id;
                            Ok(PromptStreamItem::Approval(approval))
                        }
                        _ => Err(AcpLogicError::InvalidRuntimeResult),
                    },
                    None => Ok(PromptStreamItem::Complete),
                };
                remove_active(active, session_id, cancellation_id);
                if sender.send(terminal).await.is_err() {
                    cancel_disconnected(data, cancellation_id).await;
                }
                return;
            }
            TurnDataStreamItem::Event(
                TurnDataEvent::Started
                | TurnDataEvent::ToolDelta { .. }
                | TurnDataEvent::Completed { .. },
            ) => None,
        };
        if let Some(outgoing) = outgoing
            && sender.send(Ok(outgoing)).await.is_err()
        {
            cancel_disconnected(data, cancellation_id).await;
            remove_active(active, session_id, cancellation_id);
            return;
        }
    }
    remove_active(active, session_id, cancellation_id);
    let _ = sender.send(Err(AcpLogicError::StreamClosed)).await;
}

fn remove_active(
    active: &Mutex<HashMap<SessionId, CancellationId>>,
    session_id: SessionId,
    cancellation_id: CancellationId,
) {
    if let Ok(mut active) = active.lock()
        && active.get(&session_id) == Some(&cancellation_id)
    {
        active.remove(&session_id);
    }
}

async fn cancel_disconnected<D: AcpDataPort>(data: &D, cancellation_id: CancellationId) {
    let _ = data
        .cancel(
            cancellation_id,
            String::from("ACP client disconnected during an active prompt"),
        )
        .await;
}

fn parse_session(value: &str) -> Result<SessionId, AcpLogicError> {
    SessionId::from_str(value).map_err(|_| AcpLogicError::InvalidSessionId)
}

fn same_workspace(left: &str, right: &str) -> bool {
    match (
        Path::new(left).canonicalize(),
        Path::new(right).canonicalize(),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn render_prompt(parts: Vec<PromptPart>) -> Result<String, AcpLogicError> {
    if parts.is_empty() {
        return Err(AcpLogicError::EmptyPrompt);
    }
    if parts.len() > MAX_PROMPT_PARTS {
        return Err(AcpLogicError::PromptTooLarge);
    }
    let rich = parts
        .iter()
        .any(|part| !matches!(part, PromptPart::Text(_) | PromptPart::ResourceLink { .. }));
    let mut rendered = Vec::with_capacity(parts.len());
    for part in parts {
        rendered.push(render_prompt_part(part, rich)?);
    }
    let prompt = if rich {
        serde_json::json!({
            "agentmod_acp_content_version": 1,
            "blocks": rendered,
        })
        .to_string()
    } else {
        rendered
            .into_iter()
            .map(|part| part.as_str().unwrap_or_default().to_owned())
            .collect::<Vec<_>>()
            .join("\n")
    };
    if prompt.trim().is_empty() {
        return Err(AcpLogicError::EmptyPrompt);
    }
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(AcpLogicError::PromptTooLarge);
    }
    Ok(prompt)
}

fn render_prompt_part(part: PromptPart, rich: bool) -> Result<Value, AcpLogicError> {
    match part {
        PromptPart::Text(text) => {
            validate_text(&text)?;
            Ok(if rich {
                serde_json::json!({"type": "text", "text": text})
            } else {
                Value::String(text)
            })
        }
        PromptPart::ResourceLink { name, uri } => {
            validate_label(&name)?;
            validate_uri(&uri)?;
            Ok(if rich {
                serde_json::json!({"type": "resource_link", "name": name, "uri": uri})
            } else {
                Value::String(format!("[{name}]({uri})"))
            })
        }
        PromptPart::Image {
            data,
            mime_type,
            uri,
        } => {
            validate_binary(&data)?;
            validate_mime(&mime_type, Some("image/"))?;
            if let Some(uri) = uri.as_deref() {
                validate_uri(uri)?;
            }
            Ok(serde_json::json!({
                "type": "image",
                "data": data,
                "mime_type": mime_type,
                "uri": uri,
            }))
        }
        PromptPart::Audio { data, mime_type } => {
            validate_binary(&data)?;
            validate_mime(&mime_type, Some("audio/"))?;
            Ok(serde_json::json!({
                "type": "audio",
                "data": data,
                "mime_type": mime_type,
            }))
        }
        PromptPart::EmbeddedText {
            text,
            uri,
            mime_type,
        } => {
            validate_text(&text)?;
            validate_uri(&uri)?;
            if let Some(mime_type) = mime_type.as_deref() {
                validate_mime(mime_type, None)?;
            }
            Ok(serde_json::json!({
                "type": "resource",
                "resource": {
                    "kind": "text",
                    "text": text,
                    "uri": uri,
                    "mime_type": mime_type,
                },
            }))
        }
        PromptPart::EmbeddedBlob {
            data,
            uri,
            mime_type,
        } => {
            validate_binary(&data)?;
            validate_uri(&uri)?;
            if let Some(mime_type) = mime_type.as_deref() {
                validate_mime(mime_type, None)?;
            }
            Ok(serde_json::json!({
                "type": "resource",
                "resource": {
                    "kind": "blob",
                    "data": data,
                    "uri": uri,
                    "mime_type": mime_type,
                },
            }))
        }
    }
}

fn validate_text(value: &str) -> Result<(), AcpLogicError> {
    if value.len() > MAX_TEXT_BYTES {
        return Err(AcpLogicError::PromptTooLarge);
    }
    Ok(())
}

fn validate_binary(value: &str) -> Result<(), AcpLogicError> {
    if value.len() > MAX_BINARY_BYTES.saturating_mul(4).div_ceil(3) + 4 {
        return Err(AcpLogicError::PromptTooLarge);
    }
    let decoded = BASE64
        .decode(value)
        .map_err(|_| AcpLogicError::InvalidPromptContent)?;
    if decoded.len() > MAX_BINARY_BYTES {
        return Err(AcpLogicError::PromptTooLarge);
    }
    Ok(())
}

fn validate_uri(value: &str) -> Result<(), AcpLogicError> {
    if value.is_empty()
        || value.len() > MAX_URI_BYTES
        || value.chars().any(char::is_control)
        || !value.contains(':')
    {
        return Err(AcpLogicError::InvalidPromptContent);
    }
    Ok(())
}

fn validate_label(value: &str) -> Result<(), AcpLogicError> {
    if value.trim().is_empty()
        || value.len() > MAX_LABEL_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(AcpLogicError::InvalidPromptContent);
    }
    Ok(())
}

fn validate_mime(value: &str, prefix: Option<&str>) -> Result<(), AcpLogicError> {
    if value.is_empty()
        || value.len() > MAX_MIME_BYTES
        || !value.is_ascii()
        || value.chars().any(char::is_whitespace)
        || !value.contains('/')
        || prefix.is_some_and(|prefix| !value.starts_with(prefix))
    {
        return Err(AcpLogicError::InvalidPromptContent);
    }
    Ok(())
}

fn validate_mcp_servers(servers: &[SessionMcpServer]) -> Result<(), AcpLogicError> {
    if servers.len() > MAX_MCP_SERVERS {
        return Err(AcpLogicError::InvalidMcpDeclaration);
    }
    let mut names = BTreeSet::new();
    let mut total_bytes = 0_usize;
    for server in servers {
        validate_mcp_name(&server.name)?;
        if !names.insert(server.name.to_ascii_lowercase()) {
            return Err(AcpLogicError::InvalidMcpDeclaration);
        }
        total_bytes = total_bytes
            .checked_add(server.name.len())
            .ok_or(AcpLogicError::InvalidMcpDeclaration)?;
        match &server.transport {
            SessionMcpTransport::Stdio {
                command,
                arguments,
                environment,
            } => {
                if !Path::new(command).is_absolute()
                    || command.len() > MAX_MCP_FIELD_BYTES
                    || command.contains('\0')
                    || arguments.len() > MAX_MCP_ARGUMENTS
                {
                    return Err(AcpLogicError::InvalidMcpDeclaration);
                }
                total_bytes = total_bytes
                    .checked_add(command.len())
                    .ok_or(AcpLogicError::InvalidMcpDeclaration)?;
                for argument in arguments {
                    validate_mcp_value(argument)?;
                    total_bytes = total_bytes
                        .checked_add(argument.len())
                        .ok_or(AcpLogicError::InvalidMcpDeclaration)?;
                }
                total_bytes = total_bytes
                    .checked_add(validate_mcp_key_values(environment, true)?)
                    .ok_or(AcpLogicError::InvalidMcpDeclaration)?;
            }
            SessionMcpTransport::Http { url, headers }
            | SessionMcpTransport::Sse { url, headers } => {
                validate_mcp_url(url)?;
                let header_bytes = validate_mcp_key_values(headers, false)?;
                total_bytes = total_bytes
                    .checked_add(url.len())
                    .and_then(|value| value.checked_add(header_bytes))
                    .ok_or(AcpLogicError::InvalidMcpDeclaration)?;
            }
        }
        if total_bytes > MAX_MCP_DECLARATION_BYTES {
            return Err(AcpLogicError::InvalidMcpDeclaration);
        }
    }
    Ok(())
}

fn to_data_mcp_server(server: SessionMcpServer) -> SessionMcpServerData {
    SessionMcpServerData {
        name: server.name,
        transport: match server.transport {
            SessionMcpTransport::Stdio {
                command,
                arguments,
                environment,
            } => SessionMcpTransportData::Stdio {
                program: command,
                arguments,
                environment: environment
                    .into_iter()
                    .map(|entry| SessionMcpSensitiveEntryData {
                        name: entry.name,
                        value: entry.value,
                    })
                    .collect(),
            },
            SessionMcpTransport::Http { url, headers } => SessionMcpTransportData::StreamableHttp {
                url,
                legacy_sse: false,
                headers: headers
                    .into_iter()
                    .map(|entry| SessionMcpSensitiveEntryData {
                        name: entry.name,
                        value: entry.value,
                    })
                    .collect(),
            },
            SessionMcpTransport::Sse { url, headers } => SessionMcpTransportData::StreamableHttp {
                url,
                legacy_sse: true,
                headers: headers
                    .into_iter()
                    .map(|entry| SessionMcpSensitiveEntryData {
                        name: entry.name,
                        value: entry.value,
                    })
                    .collect(),
            },
        },
    }
}

#[derive(serde::Serialize)]
struct CanonicalMcpServer<'a> {
    name: &'a str,
    transport: CanonicalMcpTransport<'a>,
}

#[derive(serde::Serialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
enum CanonicalMcpTransport<'a> {
    Stdio {
        program: &'a str,
        arguments: &'a [String],
        environment: Vec<CanonicalMcpEntry<'a>>,
    },
    StreamableHttp {
        url: &'a str,
        legacy_sse: bool,
        headers: Vec<CanonicalMcpEntry<'a>>,
    },
}

#[derive(serde::Serialize)]
struct CanonicalMcpEntry<'a> {
    name: &'a str,
    value: &'a str,
}

fn mcp_declaration_hash(servers: &[SessionMcpServer]) -> Result<String, AcpLogicError> {
    if servers.is_empty() {
        return Ok(blake3::hash(b"agentmod.session-mcp.empty@1")
            .to_hex()
            .to_string());
    }
    let canonical = servers
        .iter()
        .map(|server| CanonicalMcpServer {
            name: &server.name,
            transport: match &server.transport {
                SessionMcpTransport::Stdio {
                    command,
                    arguments,
                    environment,
                } => CanonicalMcpTransport::Stdio {
                    program: command,
                    arguments,
                    environment: environment
                        .iter()
                        .map(|entry| CanonicalMcpEntry {
                            name: &entry.name,
                            value: &entry.value,
                        })
                        .collect(),
                },
                SessionMcpTransport::Http { url, headers } => {
                    CanonicalMcpTransport::StreamableHttp {
                        url,
                        legacy_sse: false,
                        headers: headers
                            .iter()
                            .map(|entry| CanonicalMcpEntry {
                                name: &entry.name,
                                value: &entry.value,
                            })
                            .collect(),
                    }
                }
                SessionMcpTransport::Sse { url, headers } => {
                    CanonicalMcpTransport::StreamableHttp {
                        url,
                        legacy_sse: true,
                        headers: headers
                            .iter()
                            .map(|entry| CanonicalMcpEntry {
                                name: &entry.name,
                                value: &entry.value,
                            })
                            .collect(),
                    }
                }
            },
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&canonical)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|_| AcpLogicError::InvalidMcpDeclaration)
}

fn validate_mcp_name(value: &str) -> Result<(), AcpLogicError> {
    if value.trim().is_empty()
        || value.len() > MAX_MCP_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(AcpLogicError::InvalidMcpDeclaration);
    }
    Ok(())
}

fn validate_mcp_value(value: &str) -> Result<(), AcpLogicError> {
    if value.len() > MAX_MCP_FIELD_BYTES || value.contains('\0') {
        return Err(AcpLogicError::InvalidMcpDeclaration);
    }
    Ok(())
}

fn validate_mcp_key_values(
    values: &[SessionMcpKeyValue],
    environment: bool,
) -> Result<usize, AcpLogicError> {
    if values.len() > MAX_MCP_KEY_VALUES {
        return Err(AcpLogicError::InvalidMcpDeclaration);
    }
    let mut names = BTreeSet::new();
    let mut bytes = 0_usize;
    for pair in values {
        let valid_name = if environment {
            !pair.name.is_empty()
                && pair
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        } else {
            !pair.name.is_empty()
                && pair
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        };
        let normalized = pair.name.to_ascii_lowercase();
        if !valid_name || pair.name.len() > MAX_MCP_NAME_BYTES || !names.insert(normalized) {
            return Err(AcpLogicError::InvalidMcpDeclaration);
        }
        validate_mcp_value(&pair.value)?;
        bytes = bytes
            .checked_add(pair.name.len())
            .and_then(|value| value.checked_add(pair.value.len()))
            .ok_or(AcpLogicError::InvalidMcpDeclaration)?;
    }
    Ok(bytes)
}

fn validate_mcp_url(value: &str) -> Result<(), AcpLogicError> {
    if value.len() > MAX_MCP_FIELD_BYTES {
        return Err(AcpLogicError::InvalidMcpDeclaration);
    }
    let parsed = Url::parse(value).map_err(|_| AcpLogicError::InvalidMcpDeclaration)?;
    let host = parsed
        .host_str()
        .ok_or(AcpLogicError::InvalidMcpDeclaration)?;
    let secure = parsed.scheme() == "https"
        || (parsed.scheme() == "http" && matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if !secure || parsed.username() != "" || parsed.password().is_some() {
        return Err(AcpLogicError::InvalidMcpDeclaration);
    }
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the lower-layer error at this explicit boundary"
)]
fn map_error(error: AcpDataError) -> AcpLogicError {
    AcpLogicError::Data(error.to_string())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AcpLogicError {
    #[error("ACP data operation failed: {0}")]
    Data(String),
    #[error("workspace is invalid")]
    InvalidWorkspace,
    #[error("ACP session identifier is invalid")]
    InvalidSessionId,
    #[error("ACP session was not found")]
    SessionNotFound,
    #[error("ACP session workspace does not match")]
    WorkspaceMismatch,
    #[error("ACP prompt is empty")]
    EmptyPrompt,
    #[error("ACP prompt content is invalid")]
    InvalidPromptContent,
    #[error("ACP prompt exceeds the bounded content limit")]
    PromptTooLarge,
    #[error("ACP per-session MCP declaration is invalid")]
    InvalidMcpDeclaration,
    #[error("ACP per-session MCP activation is not available in this runtime")]
    McpActivationUnavailable,
    #[error("ACP MCP declarations do not match the immutable session binding")]
    McpBindingMismatch,
    #[error("ACP session already has an active prompt")]
    SessionBusy,
    #[error("ACP session has no active prompt")]
    NoActiveTurn,
    #[error("ACP runtime returned inconsistent continuation state")]
    InvalidRuntimeResult,
    #[error("ACP runtime stream closed without a terminal result")]
    StreamClosed,
    #[error("ACP runtime state is unavailable")]
    StateUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_acp_data::{SessionDataRecord, TurnDataStreamSender};

    #[derive(Clone, Default)]
    struct MockData {
        sender: Arc<Mutex<Option<TurnDataStreamSender>>>,
        cancellations: Arc<Mutex<Vec<CancellationId>>>,
        turn_cancellations: Arc<Mutex<Vec<CancellationId>>>,
        prompts: Arc<Mutex<Vec<String>>>,
        mcp_hash: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl AcpDataPort for MockData {
        async fn create_session(
            &self,
            _request: CreateSessionDataRequest,
        ) -> Result<SessionId, AcpDataError> {
            Ok(SessionId::from_uuid(Uuid::now_v7()))
        }

        async fn find_session(
            &self,
            session_id: SessionId,
        ) -> Result<Option<SessionDataRecord>, AcpDataError> {
            Ok(Some(SessionDataRecord {
                id: session_id,
                workspace: String::from("workspace"),
                mcp_declaration_hash: Some(
                    self.mcp_hash
                        .lock()
                        .expect("MCP hash")
                        .clone()
                        .unwrap_or_else(|| {
                            blake3::hash(b"agentmod.session-mcp.empty@1")
                                .to_hex()
                                .to_string()
                        }),
                ),
            }))
        }

        async fn run_turn_stream(
            &self,
            _session_id: SessionId,
            prompt: String,
            cancellation_id: CancellationId,
        ) -> Result<TurnDataStream, AcpDataError> {
            self.prompts.lock().expect("prompt lock").push(prompt);
            self.turn_cancellations
                .lock()
                .expect("turn cancellation lock")
                .push(cancellation_id);
            let (sender, stream) = TurnDataStream::channel(1);
            *self.sender.lock().expect("sender lock") = Some(sender);
            Ok(stream)
        }

        async fn resolve_approval(
            &self,
            _session_id: SessionId,
            _continuation_id: String,
            _approved: bool,
            _resume_after_resolution: bool,
        ) -> Result<Vec<TurnDataEvent>, AcpDataError> {
            Ok(Vec::new())
        }

        async fn cancel(
            &self,
            cancellation_id: CancellationId,
            _reason: String,
        ) -> Result<(), AcpDataError> {
            self.cancellations
                .lock()
                .expect("cancellation lock")
                .push(cancellation_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn forwards_updates_before_terminal_and_registers_cancellation() {
        let data = MockData::default();
        let logic = AcpLogic::new(data.clone());
        let session_id = SessionId::from_uuid(Uuid::now_v7()).to_string();
        let mut stream = logic
            .prompt_stream(PromptCommand {
                session_id: session_id.clone(),
                parts: vec![PromptPart::Text(String::from("hello"))],
            })
            .await
            .expect("prompt stream");
        let sender = data
            .sender
            .lock()
            .expect("sender lock")
            .clone()
            .expect("stream sender");
        sender
            .send(Ok(TurnDataStreamItem::Event(TurnDataEvent::Text(
                String::from("first"),
            ))))
            .await
            .expect("send text");
        assert_eq!(
            stream.recv().await.expect("update").expect("valid update"),
            PromptStreamItem::Update(PromptUpdate::Text(String::from("first")))
        );
        logic
            .cancel_session(session_id)
            .await
            .expect("active cancellation");
        {
            let turn_cancellations = data
                .turn_cancellations
                .lock()
                .expect("turn cancellation lock");
            let cancellations = data.cancellations.lock().expect("cancellation lock");
            assert_eq!(turn_cancellations.len(), 1);
            assert_eq!(cancellations.as_slice(), turn_cancellations.as_slice());
        }
        sender
            .send(Ok(TurnDataStreamItem::Complete {
                awaiting_continuation: None,
            }))
            .await
            .expect("send completion");
        assert_eq!(
            stream
                .recv()
                .await
                .expect("terminal")
                .expect("valid terminal"),
            PromptStreamItem::Complete
        );
    }

    #[tokio::test]
    async fn rejects_inconsistent_approval_continuation() {
        let data = MockData::default();
        let logic = AcpLogic::new(data.clone());
        let mut stream = logic
            .prompt_stream(PromptCommand {
                session_id: SessionId::from_uuid(Uuid::now_v7()).to_string(),
                parts: vec![PromptPart::Text(String::from("approval"))],
            })
            .await
            .expect("prompt stream");
        let sender = data
            .sender
            .lock()
            .expect("sender lock")
            .clone()
            .expect("stream sender");
        sender
            .send(Ok(TurnDataStreamItem::Complete {
                awaiting_continuation: Some(String::from("missing")),
            }))
            .await
            .expect("send completion");
        assert_eq!(
            stream.recv().await.expect("terminal"),
            Err(AcpLogicError::InvalidRuntimeResult)
        );
    }

    #[tokio::test]
    async fn rich_content_is_bounded_validated_and_preserved_as_typed_json() {
        let data = MockData::default();
        let logic = AcpLogic::new(data.clone());
        let png = BASE64.encode([0x89, b'P', b'N', b'G']);
        let audio = BASE64.encode(b"sound");
        let mut stream = logic
            .prompt_stream(PromptCommand {
                session_id: SessionId::from_uuid(Uuid::now_v7()).to_string(),
                parts: vec![
                    PromptPart::Text(String::from("inspect the attachments")),
                    PromptPart::Image {
                        data: png.clone(),
                        mime_type: String::from("image/png"),
                        uri: Some(String::from("file:///workspace/image.png")),
                    },
                    PromptPart::Audio {
                        data: audio.clone(),
                        mime_type: String::from("audio/wav"),
                    },
                    PromptPart::EmbeddedText {
                        text: String::from("embedded context"),
                        uri: String::from("file:///workspace/context.txt"),
                        mime_type: Some(String::from("text/plain")),
                    },
                    PromptPart::EmbeddedBlob {
                        data: BASE64.encode(b"blob"),
                        uri: String::from("file:///workspace/data.bin"),
                        mime_type: Some(String::from("application/octet-stream")),
                    },
                ],
            })
            .await
            .expect("rich prompt");
        let prompt = data.prompts.lock().expect("prompt lock")[0].clone();
        let value: Value = serde_json::from_str(&prompt).expect("typed prompt JSON");
        assert_eq!(value["agentmod_acp_content_version"], 1);
        assert_eq!(value["blocks"][1]["type"], "image");
        assert_eq!(value["blocks"][1]["data"], png);
        assert_eq!(value["blocks"][2]["data"], audio);
        assert_eq!(value["blocks"][3]["resource"]["kind"], "text");
        assert_eq!(value["blocks"][4]["resource"]["kind"], "blob");
        let sender = data
            .sender
            .lock()
            .expect("sender lock")
            .clone()
            .expect("stream sender");
        sender
            .send(Ok(TurnDataStreamItem::Complete {
                awaiting_continuation: None,
            }))
            .await
            .expect("completion");
        assert_eq!(
            stream
                .recv()
                .await
                .expect("terminal")
                .expect("valid terminal"),
            PromptStreamItem::Complete
        );
    }

    #[tokio::test]
    async fn malformed_or_oversized_rich_content_fails_before_runtime_dispatch() {
        let data = MockData::default();
        let logic = AcpLogic::new(data.clone());
        let session = SessionId::from_uuid(Uuid::now_v7()).to_string();
        let malformed = logic
            .prompt_stream(PromptCommand {
                session_id: session.clone(),
                parts: vec![PromptPart::Image {
                    data: String::from("not base64"),
                    mime_type: String::from("image/png"),
                    uri: None,
                }],
            })
            .await;
        assert!(matches!(
            malformed,
            Err(AcpLogicError::InvalidPromptContent)
        ));
        let oversized = logic
            .prompt_stream(PromptCommand {
                session_id: session,
                parts: vec![PromptPart::Text("x".repeat(MAX_TEXT_BYTES + 1))],
            })
            .await;
        assert!(matches!(oversized, Err(AcpLogicError::PromptTooLarge)));
        assert!(data.prompts.lock().expect("prompt lock").is_empty());
    }

    #[tokio::test]
    async fn mcp_declarations_are_bounded_and_forwarded_for_runtime_activation() {
        let data = MockData::default();
        let logic = AcpLogic::new(data);
        let command = std::env::current_exe()
            .expect("current executable")
            .to_string_lossy()
            .into_owned();
        let valid = logic
            .create_session(CreateSessionCommand {
                workspace: String::from("workspace"),
                mcp_servers: vec![SessionMcpServer {
                    name: String::from("local fixture"),
                    transport: SessionMcpTransport::Stdio {
                        command,
                        arguments: vec![String::from("--fixture")],
                        environment: vec![SessionMcpKeyValue {
                            name: String::from("FIXTURE_TOKEN"),
                            value: String::from("secret retained only in the activation request"),
                        }],
                    },
                }],
            })
            .await;
        assert!(valid.is_ok());

        let invalid = logic
            .create_session(CreateSessionCommand {
                workspace: String::from("workspace"),
                mcp_servers: vec![SessionMcpServer {
                    name: String::from("remote"),
                    transport: SessionMcpTransport::Http {
                        url: String::from("http://example.com/mcp"),
                        headers: Vec::new(),
                    },
                }],
            })
            .await;
        assert_eq!(invalid, Err(AcpLogicError::InvalidMcpDeclaration));
    }

    #[tokio::test]
    async fn load_requires_the_exact_immutable_mcp_declaration_hash() {
        let command = std::env::current_exe()
            .expect("current executable")
            .to_string_lossy()
            .into_owned();
        let servers = vec![SessionMcpServer {
            name: String::from("local fixture"),
            transport: SessionMcpTransport::Stdio {
                command,
                arguments: vec![String::from("--stdio")],
                environment: vec![SessionMcpKeyValue {
                    name: String::from("FIXTURE_TOKEN"),
                    value: String::from("exact secret"),
                }],
            },
        }];
        let data = MockData::default();
        *data.mcp_hash.lock().expect("MCP hash") =
            Some(mcp_declaration_hash(&servers).expect("declaration hash"));
        let logic = AcpLogic::new(data);
        let session_id = SessionId::from_uuid(Uuid::now_v7()).to_string();
        logic
            .load_session(LoadSessionCommand {
                session_id: session_id.clone(),
                workspace: String::from("workspace"),
                mcp_servers: servers.clone(),
            })
            .await
            .expect("exact binding");
        let mut substituted = servers;
        let SessionMcpTransport::Stdio { environment, .. } = &mut substituted[0].transport else {
            panic!("stdio fixture")
        };
        environment[0].value = String::from("substituted secret");
        assert_eq!(
            logic
                .load_session(LoadSessionCommand {
                    session_id,
                    workspace: String::from("workspace"),
                    mcp_servers: substituted,
                })
                .await,
            Err(AcpLogicError::McpBindingMismatch)
        );
    }

    #[test]
    fn mcp_sensitive_values_are_redacted_from_layer_owned_diagnostics() {
        let declaration = SessionMcpServer {
            name: String::from("fixture"),
            transport: SessionMcpTransport::Http {
                url: String::from("https://example.test/mcp"),
                headers: vec![SessionMcpKeyValue {
                    name: String::from("Authorization"),
                    value: String::from("Bearer never-log-this"),
                }],
            },
        };
        let debug = format!("{declaration:?}");
        assert!(debug.contains("Authorization"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("never-log-this"));
    }
}
