//! ACP-owned runtime transport adapter.
#![allow(
    missing_docs,
    reason = "dependency-local records are boundary-specific"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the dependency port exposes one closed error taxonomy"
)]

use std::{collections::BTreeSet, sync::Arc};

use agentmod_primitives::{
    CancellationId, CausationId, CorrelationId, IdempotencyId, RequestId, SessionId, Version,
};
use agentmod_protocol_support::{
    DEFAULT_MAX_FRAME_BYTES, FrameHeader, FrameKind, Handshake, Negotiated, WireFrame, read_frame,
    write_frame,
};
use agentmod_runtime_protocol::{
    RuntimeMcpSensitiveEntry, RuntimeMcpServerDeclaration, RuntimeMcpTransportDeclaration,
    RuntimeProviderEvent, RuntimeRequest, RuntimeResponse,
};
use async_trait::async_trait;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use uuid::Uuid;

const RUNTIME_PROTOCOL_VERSION: Version = Version::new(2, 5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySession {
    pub id: SessionId,
    pub workspace: String,
    pub mcp_declaration_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateSessionRequest {
    pub workspace: String,
    pub style: String,
    pub mcp_servers: Vec<DependencyMcpServer>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyMcpServer {
    pub name: String,
    pub transport: DependencyMcpTransport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyMcpTransport {
    Stdio {
        program: String,
        arguments: Vec<String>,
        environment: Vec<DependencyMcpSensitiveEntry>,
    },
    StreamableHttp {
        url: String,
        legacy_sse: bool,
        headers: Vec<DependencyMcpSensitiveEntry>,
    },
}

#[derive(Clone, Eq, PartialEq)]
pub struct DependencyMcpSensitiveEntry {
    pub name: String,
    pub value: String,
}

impl std::fmt::Debug for DependencyMcpSensitiveEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DependencyMcpSensitiveEntry")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyTurnEvent {
    Started,
    Text(String),
    ToolDelta {
        call_id: String,
        name: String,
        arguments: String,
    },
    ToolProposed {
        continuation_id: String,
        call_id: String,
        tool: String,
        arguments: Value,
    },
    Completed {
        reason: String,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyTurnStreamItem {
    Event(DependencyTurnEvent),
    Complete {
        awaiting_continuation: Option<String>,
    },
}

pub struct DependencyTurnStream {
    receiver: mpsc::Receiver<Result<DependencyTurnStreamItem, AcpDependencyError>>,
}

#[derive(Clone)]
pub struct DependencyTurnStreamSender {
    sender: mpsc::Sender<Result<DependencyTurnStreamItem, AcpDependencyError>>,
}

impl DependencyTurnStream {
    #[must_use]
    pub fn channel(capacity: usize) -> (DependencyTurnStreamSender, Self) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        (DependencyTurnStreamSender { sender }, Self { receiver })
    }

    pub async fn recv(&mut self) -> Option<Result<DependencyTurnStreamItem, AcpDependencyError>> {
        self.receiver.recv().await
    }
}

impl DependencyTurnStreamSender {
    pub async fn send(
        &self,
        item: Result<DependencyTurnStreamItem, AcpDependencyError>,
    ) -> Result<(), AcpDependencyError> {
        self.sender
            .send(item)
            .await
            .map_err(|_| AcpDependencyError::Cancelled)
    }
}

#[async_trait]
pub trait AcpRuntimeDependencyPort: Send + Sync {
    async fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<SessionId, AcpDependencyError>;
    async fn find_session(
        &self,
        session_id: SessionId,
    ) -> Result<Option<DependencySession>, AcpDependencyError>;
    async fn run_turn_stream(
        &self,
        session_id: SessionId,
        prompt: String,
        cancellation_id: CancellationId,
    ) -> Result<DependencyTurnStream, AcpDependencyError>;
    async fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<Vec<DependencyTurnEvent>, AcpDependencyError>;
    async fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), AcpDependencyError>;
}

#[derive(Clone, Debug)]
pub struct LocalRuntimeDependency {
    endpoint: String,
    authorization_token: Arc<str>,
    maximum_frame_bytes: usize,
    provider: Arc<str>,
    model: Arc<str>,
    provider_options: Value,
}

impl LocalRuntimeDependency {
    pub fn new(
        endpoint: String,
        authorization_token: String,
        maximum_frame_bytes: usize,
    ) -> Result<Self, AcpDependencyError> {
        if endpoint.trim().is_empty()
            || authorization_token.len() < 32
            || maximum_frame_bytes == 0
            || maximum_frame_bytes > DEFAULT_MAX_FRAME_BYTES
        {
            return Err(AcpDependencyError::InvalidConfiguration);
        }
        Ok(Self {
            endpoint,
            authorization_token: authorization_token.into(),
            maximum_frame_bytes,
            provider: String::from("deterministic-mock").into(),
            model: String::from("mock-model").into(),
            provider_options: Value::Object(Map::default()),
        })
    }

    pub fn with_provider_request(
        mut self,
        provider: String,
        model: String,
        options: Value,
    ) -> Result<Self, AcpDependencyError> {
        if provider.trim().is_empty() || model.trim().is_empty() || !options.is_object() {
            return Err(AcpDependencyError::InvalidConfiguration);
        }
        self.provider = provider.into();
        self.model = model.into();
        self.provider_options = options;
        Ok(self)
    }

    #[cfg(unix)]
    async fn send(&self, request: RuntimeRequest) -> Result<RuntimeResponse, AcpDependencyError> {
        let mut stream = tokio::net::UnixStream::connect(&self.endpoint)
            .await
            .map_err(|_| AcpDependencyError::Transport)?;
        self.exchange(&mut stream, request).await
    }

    #[cfg(windows)]
    async fn send(&self, request: RuntimeRequest) -> Result<RuntimeResponse, AcpDependencyError> {
        let mut stream = self.open_windows_pipe().await?;
        self.exchange(&mut stream, request).await
    }

    #[cfg(windows)]
    async fn open_windows_pipe(
        &self,
    ) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, AcpDependencyError> {
        for attempt in 0..200_u16 {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(&self.endpoint) {
                Ok(stream) => return Ok(stream),
                Err(_) if attempt < 199 => {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(_) => return Err(AcpDependencyError::Transport),
            }
        }
        Err(AcpDependencyError::Transport)
    }

    async fn exchange<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
    ) -> Result<RuntimeResponse, AcpDependencyError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (request_header, _credit_windows) = self.negotiate(stream, request).await?;
        let current: WireFrame<RuntimeResponse> = read_frame(stream, self.maximum_frame_bytes)
            .await
            .map_err(|_| AcpDependencyError::Transport)?;
        if current.header.kind == FrameKind::Response {
            validate_response_header(&current.header, &request_header)?;
            return Ok(current.payload);
        }
        Err(AcpDependencyError::UnexpectedResponse)
    }

    async fn exchange_turn<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
        sender: &mpsc::Sender<Result<DependencyTurnStreamItem, AcpDependencyError>>,
    ) -> Result<(), AcpDependencyError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (request_header, credit_windows) = self.negotiate(stream, request).await?;
        let mut expected_sequence = 1_u64;
        loop {
            let current: WireFrame<RuntimeResponse> = read_frame(stream, self.maximum_frame_bytes)
                .await
                .map_err(|_| AcpDependencyError::Transport)?;
            validate_stream_header(&current.header, &request_header, expected_sequence)?;
            let item = match current.header.kind {
                FrameKind::StreamItem => {
                    let RuntimeResponse::TurnEvent { event, .. } = current.payload else {
                        return Err(AcpDependencyError::UnexpectedResponse);
                    };
                    DependencyTurnStreamItem::Event(map_event(event))
                }
                FrameKind::StreamEnd => {
                    let RuntimeResponse::TurnComplete {
                        awaiting_continuation,
                        ..
                    } = current.payload
                    else {
                        return Err(AcpDependencyError::UnexpectedResponse);
                    };
                    sender
                        .send(Ok(DependencyTurnStreamItem::Complete {
                            awaiting_continuation,
                        }))
                        .await
                        .map_err(|_| AcpDependencyError::Cancelled)?;
                    return Ok(());
                }
                _ => return Err(AcpDependencyError::Protocol),
            };
            sender
                .send(Ok(item))
                .await
                .map_err(|_| AcpDependencyError::Cancelled)?;
            if credit_windows {
                write_window_update(
                    stream,
                    &request_header,
                    expected_sequence,
                    self.maximum_frame_bytes,
                )
                .await?;
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(AcpDependencyError::Protocol)?;
        }
    }

    #[cfg(unix)]
    async fn stream_turn(
        &self,
        request: RuntimeRequest,
        sender: &mpsc::Sender<Result<DependencyTurnStreamItem, AcpDependencyError>>,
    ) -> Result<(), AcpDependencyError> {
        let mut stream = tokio::net::UnixStream::connect(&self.endpoint)
            .await
            .map_err(|_| AcpDependencyError::Transport)?;
        self.exchange_turn(&mut stream, request, sender).await
    }

    #[cfg(windows)]
    async fn stream_turn(
        &self,
        request: RuntimeRequest,
        sender: &mpsc::Sender<Result<DependencyTurnStreamItem, AcpDependencyError>>,
    ) -> Result<(), AcpDependencyError> {
        let mut stream = self.open_windows_pipe().await?;
        self.exchange_turn(&mut stream, request, sender).await
    }

    async fn negotiate<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
    ) -> Result<(FrameHeader, bool), AcpDependencyError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let handshake_header = new_header(FrameKind::Handshake);
        write_frame(
            stream,
            &WireFrame {
                header: handshake_header.clone(),
                payload: Handshake {
                    supported_versions: vec![RUNTIME_PROTOCOL_VERSION],
                    capabilities: BTreeSet::from([
                        String::from("bounded_backpressure"),
                        String::from("cancellation"),
                        String::from("credit_windows"),
                        String::from("request_response"),
                        String::from("streaming"),
                    ]),
                    authorization_token: self.authorization_token.to_string(),
                },
            },
            self.maximum_frame_bytes,
        )
        .await
        .map_err(|_| AcpDependencyError::Transport)?;
        let negotiated: WireFrame<Negotiated> = read_frame(stream, self.maximum_frame_bytes)
            .await
            .map_err(|_| AcpDependencyError::Transport)?;
        validate_response_header(&negotiated.header, &handshake_header)?;
        if !negotiated
            .payload
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        {
            return Err(AcpDependencyError::Protocol);
        }
        let credit_windows = negotiated.payload.capabilities.contains("credit_windows");
        let request_header = new_header(FrameKind::Request);
        write_frame(
            stream,
            &WireFrame {
                header: request_header.clone(),
                payload: request,
            },
            self.maximum_frame_bytes,
        )
        .await
        .map_err(|_| AcpDependencyError::Transport)?;
        Ok((request_header, credit_windows))
    }
}

#[async_trait]
impl AcpRuntimeDependencyPort for LocalRuntimeDependency {
    async fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<SessionId, AcpDependencyError> {
        let RuntimeResponse::SessionCreated { session_id } = self
            .send(RuntimeRequest::CreateSessionWithMcp {
                workspace: request.workspace,
                style: request.style,
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
                mcp_servers: request
                    .mcp_servers
                    .into_iter()
                    .map(|server| RuntimeMcpServerDeclaration {
                        name: server.name,
                        transport: match server.transport {
                            DependencyMcpTransport::Stdio {
                                program,
                                arguments,
                                environment,
                            } => RuntimeMcpTransportDeclaration::Stdio {
                                program,
                                arguments,
                                environment: environment
                                    .into_iter()
                                    .map(|entry| RuntimeMcpSensitiveEntry {
                                        name: entry.name,
                                        value: entry.value,
                                    })
                                    .collect(),
                            },
                            DependencyMcpTransport::StreamableHttp {
                                url,
                                legacy_sse,
                                headers,
                            } => RuntimeMcpTransportDeclaration::StreamableHttp {
                                url,
                                legacy_sse,
                                headers: headers
                                    .into_iter()
                                    .map(|entry| RuntimeMcpSensitiveEntry {
                                        name: entry.name,
                                        value: entry.value,
                                    })
                                    .collect(),
                            },
                        },
                    })
                    .collect(),
            })
            .await?
        else {
            return Err(AcpDependencyError::UnexpectedResponse);
        };
        Ok(session_id)
    }

    async fn find_session(
        &self,
        session_id: SessionId,
    ) -> Result<Option<DependencySession>, AcpDependencyError> {
        let RuntimeResponse::Sessions { sessions } = self
            .send(RuntimeRequest::ListSessions { limit: 1024 })
            .await?
        else {
            return Err(AcpDependencyError::UnexpectedResponse);
        };
        let Some(summary) = sessions.into_iter().find(|value| value.id == session_id) else {
            return Ok(None);
        };
        let RuntimeResponse::SessionInspected { state, .. } = self
            .send(RuntimeRequest::InspectSession {
                session_id,
                at: None,
            })
            .await?
        else {
            return Err(AcpDependencyError::UnexpectedResponse);
        };
        let mcp_declaration_hash = state
            .get("style_binding")
            .and_then(|binding| binding.get("mcp"))
            .and_then(|mcp| mcp.get("declaration_hash"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let workspace = state
            .get("workspace")
            .and_then(Value::as_str)
            .map_or(summary.workspace_label, str::to_owned);
        Ok(Some(DependencySession {
            id: summary.id,
            workspace,
            mcp_declaration_hash,
        }))
    }

    async fn run_turn_stream(
        &self,
        session_id: SessionId,
        prompt: String,
        cancellation_id: CancellationId,
    ) -> Result<DependencyTurnStream, AcpDependencyError> {
        let request = RuntimeRequest::RunTurn {
            session_id,
            prompt,
            provider: self.provider.to_string(),
            model: self.model.to_string(),
            options: self.provider_options.clone(),
            cancellation_id,
        };
        let dependency = self.clone();
        let (sender, stream) = DependencyTurnStream::channel(1);
        tokio::spawn(async move {
            if let Err(error) = dependency.stream_turn(request, &sender.sender).await {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(stream)
    }

    async fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<Vec<DependencyTurnEvent>, AcpDependencyError> {
        let RuntimeResponse::ApprovalResolved { events, .. } = self
            .send(RuntimeRequest::ResolveApproval {
                session_id,
                continuation_id,
                approved,
                resume_after_resolution,
            })
            .await?
        else {
            return Err(AcpDependencyError::UnexpectedResponse);
        };
        Ok(events.into_iter().map(map_event).collect())
    }

    async fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), AcpDependencyError> {
        let RuntimeResponse::Cancelled = self
            .send(RuntimeRequest::Cancel {
                cancellation_id,
                reason,
            })
            .await?
        else {
            return Err(AcpDependencyError::UnexpectedResponse);
        };
        Ok(())
    }
}

fn map_event(event: RuntimeProviderEvent) -> DependencyTurnEvent {
    match event {
        RuntimeProviderEvent::Started => DependencyTurnEvent::Started,
        RuntimeProviderEvent::Text { text } => DependencyTurnEvent::Text(text),
        RuntimeProviderEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => DependencyTurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        RuntimeProviderEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => DependencyTurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        RuntimeProviderEvent::Completed { reason, .. } => DependencyTurnEvent::Completed { reason },
        RuntimeProviderEvent::Cancelled => DependencyTurnEvent::Cancelled,
        RuntimeProviderEvent::Failed {
            code,
            message,
            retryable,
        } => DependencyTurnEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

fn new_header(kind: FrameKind) -> FrameHeader {
    FrameHeader {
        family: String::from("runtime"),
        version: RUNTIME_PROTOCOL_VERSION,
        kind,
        request_id: RequestId::from_uuid(Uuid::now_v7()),
        stream_sequence: None,
        correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
        causation_id: CausationId::from_uuid(Uuid::now_v7()),
        idempotency_id: IdempotencyId::from_uuid(Uuid::now_v7()),
        cancellation_id: None,
    }
}

async fn write_window_update<S>(
    stream: &mut S,
    request: &FrameHeader,
    sequence: u64,
    maximum_frame_bytes: usize,
) -> Result<(), AcpDependencyError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut header = request.clone();
    header.kind = FrameKind::WindowUpdate;
    header.stream_sequence = Some(sequence);
    write_frame(
        stream,
        &WireFrame {
            header,
            payload: RuntimeRequest::StreamWindowUpdate {
                credits: 1,
                last_received_sequence: sequence,
            },
        },
        maximum_frame_bytes,
    )
    .await
    .map_err(|_| AcpDependencyError::Transport)
}

fn validate_response_header(
    response: &FrameHeader,
    request: &FrameHeader,
) -> Result<(), AcpDependencyError> {
    if response.family != "runtime"
        || response.kind != FrameKind::Response
        || !response
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        || response.request_id != request.request_id
        || response.correlation_id != request.correlation_id
        || response.idempotency_id != request.idempotency_id
    {
        return Err(AcpDependencyError::Protocol);
    }
    Ok(())
}

fn validate_stream_header(
    response: &FrameHeader,
    request: &FrameHeader,
    expected_sequence: u64,
) -> Result<(), AcpDependencyError> {
    if response.family != "runtime"
        || !matches!(response.kind, FrameKind::StreamItem | FrameKind::StreamEnd)
        || response.stream_sequence != Some(expected_sequence)
        || !response
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        || response.request_id != request.request_id
        || response.correlation_id != request.correlation_id
        || response.idempotency_id != request.idempotency_id
    {
        return Err(AcpDependencyError::Protocol);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AcpDependencyError {
    #[error("ACP runtime dependency configuration is invalid")]
    InvalidConfiguration,
    #[error("ACP runtime transport is unavailable")]
    Transport,
    #[error("ACP runtime protocol validation failed")]
    Protocol,
    #[error("runtime returned an unexpected ACP response")]
    UnexpectedResponse,
    #[error("ACP runtime stream consumer disconnected")]
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_is_fail_closed() {
        assert!(matches!(
            LocalRuntimeDependency::new(String::new(), String::from("short"), 0),
            Err(AcpDependencyError::InvalidConfiguration)
        ));
    }

    #[tokio::test]
    async fn negotiation_uses_current_runtime_protocol_and_preserves_request() {
        let dependency = LocalRuntimeDependency::new(
            String::from("fixture"),
            "x".repeat(32),
            DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("dependency");
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        let peer = tokio::spawn(async move {
            let handshake: WireFrame<Handshake> = read_frame(&mut server, DEFAULT_MAX_FRAME_BYTES)
                .await
                .expect("handshake");
            assert_eq!(
                handshake.payload.supported_versions,
                vec![Version::new(2, 5)]
            );
            let mut response = handshake.header;
            response.kind = FrameKind::Response;
            write_frame(
                &mut server,
                &WireFrame {
                    header: response,
                    payload: Negotiated {
                        version: Version::new(2, 5),
                        capabilities: BTreeSet::from([String::from("credit_windows")]),
                    },
                },
                DEFAULT_MAX_FRAME_BYTES,
            )
            .await
            .expect("negotiated");
            let request: WireFrame<RuntimeRequest> =
                read_frame(&mut server, DEFAULT_MAX_FRAME_BYTES)
                    .await
                    .expect("request");
            assert_eq!(request.payload, RuntimeRequest::Health);
        });
        let (_, credits) = dependency
            .negotiate(&mut client, RuntimeRequest::Health)
            .await
            .expect("negotiation");
        assert!(credits);
        peer.await.expect("peer");
    }
}
