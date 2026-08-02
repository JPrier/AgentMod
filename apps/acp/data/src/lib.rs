//! ACP business datasets and explicit runtime-dependency normalization.
#![allow(missing_docs, reason = "data-local records are boundary-specific")]
#![allow(
    async_fn_in_trait,
    reason = "the first-party ACP data boundary intentionally uses static async dispatch"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the data port exposes one closed error taxonomy"
)]

use agentmod_acp_dependency::{
    AcpDependencyError, AcpRuntimeDependencyPort, DependencyCreateSessionRequest,
    DependencyMcpSensitiveEntry, DependencyMcpServer, DependencyMcpTransport, DependencyTurnEvent,
    DependencyTurnStreamItem,
};
use agentmod_primitives::{CancellationId, SessionId};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionDataRecord {
    pub id: SessionId,
    pub workspace: String,
    pub mcp_declaration_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionDataRequest {
    pub workspace: String,
    pub style: String,
    pub mcp_servers: Vec<SessionMcpServerData>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionMcpServerData {
    pub name: String,
    pub transport: SessionMcpTransportData,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionMcpTransportData {
    Stdio {
        program: String,
        arguments: Vec<String>,
        environment: Vec<SessionMcpSensitiveEntryData>,
    },
    StreamableHttp {
        url: String,
        legacy_sse: bool,
        headers: Vec<SessionMcpSensitiveEntryData>,
    },
}

#[derive(Clone, Eq, PartialEq)]
pub struct SessionMcpSensitiveEntryData {
    pub name: String,
    pub value: String,
}

impl std::fmt::Debug for SessionMcpSensitiveEntryData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionMcpSensitiveEntryData")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnDataEvent {
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
pub enum TurnDataStreamItem {
    Event(TurnDataEvent),
    Complete {
        awaiting_continuation: Option<String>,
    },
}

pub struct TurnDataStream {
    receiver: mpsc::Receiver<Result<TurnDataStreamItem, AcpDataError>>,
}

#[derive(Clone)]
pub struct TurnDataStreamSender {
    sender: mpsc::Sender<Result<TurnDataStreamItem, AcpDataError>>,
}

impl TurnDataStream {
    #[must_use]
    pub fn channel(capacity: usize) -> (TurnDataStreamSender, Self) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        (TurnDataStreamSender { sender }, Self { receiver })
    }

    pub async fn recv(&mut self) -> Option<Result<TurnDataStreamItem, AcpDataError>> {
        self.receiver.recv().await
    }
}

impl TurnDataStreamSender {
    pub async fn send(
        &self,
        item: Result<TurnDataStreamItem, AcpDataError>,
    ) -> Result<(), AcpDataError> {
        self.sender
            .send(item)
            .await
            .map_err(|_| AcpDataError::StreamClosed)
    }
}

#[async_trait]
pub trait AcpDataPort: Send + Sync {
    async fn create_session(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<SessionId, AcpDataError>;
    async fn find_session(
        &self,
        session_id: SessionId,
    ) -> Result<Option<SessionDataRecord>, AcpDataError>;
    async fn run_turn_stream(
        &self,
        session_id: SessionId,
        prompt: String,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataStream, AcpDataError>;
    async fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
        resume_after_resolution: bool,
    ) -> Result<Vec<TurnDataEvent>, AcpDataError>;
    async fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), AcpDataError>;
}

#[derive(Clone, Debug)]
pub struct AcpData<D> {
    dependency: D,
}

impl<D> AcpData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: AcpRuntimeDependencyPort> AcpDataPort for AcpData<D> {
    async fn create_session(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<SessionId, AcpDataError> {
        self.dependency
            .create_session(DependencyCreateSessionRequest {
                workspace: request.workspace,
                style: request.style,
                mcp_servers: request
                    .mcp_servers
                    .into_iter()
                    .map(|server| DependencyMcpServer {
                        name: server.name,
                        transport: match server.transport {
                            SessionMcpTransportData::Stdio {
                                program,
                                arguments,
                                environment,
                            } => DependencyMcpTransport::Stdio {
                                program,
                                arguments,
                                environment: environment
                                    .into_iter()
                                    .map(|entry| DependencyMcpSensitiveEntry {
                                        name: entry.name,
                                        value: entry.value,
                                    })
                                    .collect(),
                            },
                            SessionMcpTransportData::StreamableHttp {
                                url,
                                legacy_sse,
                                headers,
                            } => DependencyMcpTransport::StreamableHttp {
                                url,
                                legacy_sse,
                                headers: headers
                                    .into_iter()
                                    .map(|entry| DependencyMcpSensitiveEntry {
                                        name: entry.name,
                                        value: entry.value,
                                    })
                                    .collect(),
                            },
                        },
                    })
                    .collect(),
            })
            .await
            .map_err(map_error)
    }

    async fn find_session(
        &self,
        session_id: SessionId,
    ) -> Result<Option<SessionDataRecord>, AcpDataError> {
        self.dependency
            .find_session(session_id)
            .await
            .map(|value| {
                value.map(|session| SessionDataRecord {
                    id: session.id,
                    workspace: session.workspace,
                    mcp_declaration_hash: session.mcp_declaration_hash,
                })
            })
            .map_err(map_error)
    }

    async fn run_turn_stream(
        &self,
        session_id: SessionId,
        prompt: String,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataStream, AcpDataError> {
        let mut dependency_stream = self
            .dependency
            .run_turn_stream(session_id, prompt, cancellation_id)
            .await
            .map_err(map_error)?;
        let (sender, stream) = TurnDataStream::channel(1);
        tokio::spawn(async move {
            while let Some(item) = dependency_stream.recv().await {
                let item = item.map(map_stream_item).map_err(map_error);
                if sender.send(item).await.is_err() {
                    return;
                }
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
    ) -> Result<Vec<TurnDataEvent>, AcpDataError> {
        self.dependency
            .resolve_approval(
                session_id,
                continuation_id,
                approved,
                resume_after_resolution,
            )
            .await
            .map(|events| events.into_iter().map(map_event).collect())
            .map_err(map_error)
    }

    async fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), AcpDataError> {
        self.dependency
            .cancel(cancellation_id, reason)
            .await
            .map_err(map_error)
    }
}

fn map_event(event: DependencyTurnEvent) -> TurnDataEvent {
    match event {
        DependencyTurnEvent::Started => TurnDataEvent::Started,
        DependencyTurnEvent::Text(value) => TurnDataEvent::Text(value),
        DependencyTurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => TurnDataEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        DependencyTurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => TurnDataEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        DependencyTurnEvent::Completed { reason } => TurnDataEvent::Completed { reason },
        DependencyTurnEvent::Cancelled => TurnDataEvent::Cancelled,
        DependencyTurnEvent::Failed {
            code,
            message,
            retryable,
        } => TurnDataEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

fn map_stream_item(item: DependencyTurnStreamItem) -> TurnDataStreamItem {
    match item {
        DependencyTurnStreamItem::Event(event) => TurnDataStreamItem::Event(map_event(event)),
        DependencyTurnStreamItem::Complete {
            awaiting_continuation,
        } => TurnDataStreamItem::Complete {
            awaiting_continuation,
        },
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the lower-layer error at this explicit boundary"
)]
fn map_error(error: AcpDependencyError) -> AcpDataError {
    AcpDataError::Dependency(error.to_string())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AcpDataError {
    #[error("ACP runtime dependency failed: {0}")]
    Dependency(String),
    #[error("ACP data stream consumer disconnected")]
    StreamClosed,
}
