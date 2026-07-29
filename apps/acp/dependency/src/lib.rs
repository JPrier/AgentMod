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
use agentmod_runtime_protocol::{RuntimeProviderEvent, RuntimeRequest, RuntimeResponse};
use async_trait::async_trait;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use uuid::Uuid;

const RUNTIME_PROTOCOL_VERSION: Version = Version::new(2, 4);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySession {
    pub id: SessionId,
    pub workspace: String,
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
        workspace: String,
        style: String,
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
        let mut stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&self.endpoint)
            .map_err(|_| AcpDependencyError::Transport)?;
        self.exchange(&mut stream, request).await
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
        let mut stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&self.endpoint)
            .map_err(|_| AcpDependencyError::Transport)?;
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
        workspace: String,
        style: String,
    ) -> Result<SessionId, AcpDependencyError> {
        let RuntimeResponse::SessionCreated { session_id } = self
            .send(RuntimeRequest::CreateSession {
                workspace,
                style,
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
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
        Ok(sessions
            .into_iter()
            .find(|value| value.id == session_id)
            .map(|value| DependencySession {
                id: value.id,
                workspace: value.workspace_label,
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
}
