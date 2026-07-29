//! TUI-owned authenticated runtime transport adapter.
#![allow(
    missing_docs,
    reason = "dependency-local frontend records are exhaustively named and mapped"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the dependency port exposes one documented closed error taxonomy"
)]

use std::{
    collections::BTreeSet,
    sync::{Arc, mpsc},
};

use agentmod_primitives::{
    CancellationId, CausationId, CorrelationId, IdempotencyId, RequestId, Sequence, SessionId,
    Version,
};
use agentmod_protocol_support::{
    DEFAULT_MAX_FRAME_BYTES, FrameHeader, FrameKind, Handshake, Negotiated, WireFrame, read_frame,
    write_frame,
};
use agentmod_runtime_protocol::{
    RuntimeExecutionBudgetOverrides, RuntimeProviderEvent, RuntimeRequest, RuntimeResponse,
    RuntimeSessionEvent, RuntimeStyleAvailability, RuntimeStyleSourceKind,
};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

const RUNTIME_PROTOCOL_VERSION: Version = Version::new(2, 4);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRuntimeHealth {
    pub status: String,
    pub version: String,
}

/// Dependency-owned optional session budget selection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DependencySessionBudgetSelection {
    pub max_iterations: Option<u32>,
    pub max_steps: Option<u64>,
    pub max_tokens: Option<u64>,
    pub max_cost_micros: Option<u64>,
    pub max_duration_ms: Option<u64>,
}

/// Dependency-owned complete session creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateSessionRequest {
    pub workspace: String,
    pub style: String,
    pub harness: Option<String>,
    pub memory: Option<String>,
    pub compaction: Option<String>,
    pub budgets: Option<DependencySessionBudgetSelection>,
}

/// Dependency-owned provenance for one registry style.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyStyleSourceKind {
    BuiltIn,
    User,
    Project,
    Plugin,
    Inline,
}

/// Dependency-owned availability for one registry style.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyStyleAvailability {
    Available,
    Disabled,
    Invalid,
    Incompatible,
    Conflict,
}

/// Dependency-owned bounded style catalog row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleSummary {
    pub id: String,
    pub version: String,
    pub source: DependencyStyleSourceKind,
    pub availability: DependencyStyleAvailability,
    pub style_content_hash: String,
    pub compiled_cache_key: String,
    pub required_capabilities: Vec<String>,
}

/// Dependency-owned harness descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyHarnessDescriptor {
    pub id: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub capability_set_hash: String,
    pub availability: String,
}

/// Dependency-owned style-selectable component catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySessionComponentCatalog {
    pub memory_providers: Vec<String>,
    pub compaction_strategies: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySessionSummary {
    pub id: SessionId,
    pub workspace: String,
    pub style: String,
    pub sequence: Sequence,
    pub state: String,
}

/// Dependency-owned atomic branch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyBranchSessionRequest {
    pub parent_session_id: SessionId,
    pub at: Sequence,
    pub style: Option<String>,
}

/// Dependency-owned atomic branch result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyBranchSessionResponse {
    pub session_id: SessionId,
    pub parent_session_id: SessionId,
    pub fork_sequence: Sequence,
    pub child_head_sequence: Sequence,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencySessionEvent {
    pub sequence: Sequence,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencySessionEventPage {
    pub events: Vec<DependencySessionEvent>,
    pub head_sequence: Sequence,
    pub last_delivered_sequence: Option<Sequence>,
    pub has_more: bool,
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
        input_tokens: u64,
        output_tokens: u64,
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
    Event {
        event: DependencyTurnEvent,
        committed_sequence: Sequence,
    },
    Complete {
        first_committed_sequence: Sequence,
        last_committed_sequence: Sequence,
        awaiting_continuation: Option<String>,
    },
}

pub struct DependencyTurnStream {
    receiver: mpsc::Receiver<Result<DependencyTurnStreamItem, TuiDependencyError>>,
}

impl DependencyTurnStream {
    #[must_use]
    pub fn try_next(&self) -> Option<Result<DependencyTurnStreamItem, TuiDependencyError>> {
        match self.receiver.try_recv() {
            Ok(value) => Some(value),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => Some(Err(TuiDependencyError::Transport)),
        }
    }
}

pub trait TuiRuntimeDependencyPort: Send + Sync {
    fn health(&self) -> Result<DependencyRuntimeHealth, TuiDependencyError>;
    fn list_styles(&self) -> Result<Vec<DependencyStyleSummary>, TuiDependencyError>;
    fn list_harnesses(&self) -> Result<Vec<DependencyHarnessDescriptor>, TuiDependencyError> {
        Ok(Vec::new())
    }
    fn list_session_components(
        &self,
    ) -> Result<DependencySessionComponentCatalog, TuiDependencyError> {
        Ok(DependencySessionComponentCatalog {
            memory_providers: Vec::new(),
            compaction_strategies: Vec::new(),
        })
    }
    fn list_sessions(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencySessionSummary>, TuiDependencyError>;
    fn inspect_session(&self, _session_id: SessionId) -> Result<Value, TuiDependencyError> {
        Ok(Value::Null)
    }
    fn create_session(
        &self,
        workspace: String,
        style: String,
    ) -> Result<SessionId, TuiDependencyError>;
    fn create_session_with_harness(
        &self,
        workspace: String,
        style: String,
        _harness: Option<String>,
    ) -> Result<SessionId, TuiDependencyError> {
        self.create_session(workspace, style)
    }
    fn create_session_with_components(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
        memory: Option<String>,
        compaction: Option<String>,
    ) -> Result<SessionId, TuiDependencyError> {
        let _ = (memory, compaction);
        self.create_session_with_harness(workspace, style, harness)
    }
    fn create_session_with_configuration(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<SessionId, TuiDependencyError> {
        let _ = request.budgets;
        self.create_session_with_components(
            request.workspace,
            request.style,
            request.harness,
            request.memory,
            request.compaction,
        )
    }
    fn branch_session(
        &self,
        request: DependencyBranchSessionRequest,
    ) -> Result<DependencyBranchSessionResponse, TuiDependencyError>;
    fn session_events(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
        limit: u32,
    ) -> Result<DependencySessionEventPage, TuiDependencyError>;
    fn start_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        provider: String,
        model: String,
        options: Value,
        cancellation_id: CancellationId,
    ) -> Result<DependencyTurnStream, TuiDependencyError>;
    fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<DependencyTurnEvent>, TuiDependencyError>;
    fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), TuiDependencyError>;
}

#[derive(Clone, Debug)]
pub struct LocalRuntimeDependency {
    endpoint: String,
    authorization_token: Arc<str>,
    maximum_frame_bytes: usize,
}

impl LocalRuntimeDependency {
    pub fn new(
        endpoint: String,
        authorization_token: String,
        maximum_frame_bytes: usize,
    ) -> Result<Self, TuiDependencyError> {
        if endpoint.trim().is_empty()
            || authorization_token.len() < 32
            || maximum_frame_bytes == 0
            || maximum_frame_bytes > DEFAULT_MAX_FRAME_BYTES
        {
            return Err(TuiDependencyError::InvalidConfiguration);
        }
        Ok(Self {
            endpoint,
            authorization_token: authorization_token.into(),
            maximum_frame_bytes,
        })
    }

    fn send(&self, request: RuntimeRequest) -> Result<RuntimeResponse, TuiDependencyError> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| TuiDependencyError::Transport)?
            .block_on(self.send_async(request))
    }

    #[cfg(unix)]
    async fn send_async(
        &self,
        request: RuntimeRequest,
    ) -> Result<RuntimeResponse, TuiDependencyError> {
        let mut stream = tokio::net::UnixStream::connect(&self.endpoint)
            .await
            .map_err(|_| TuiDependencyError::Transport)?;
        self.exchange(&mut stream, request).await
    }

    #[cfg(windows)]
    async fn send_async(
        &self,
        request: RuntimeRequest,
    ) -> Result<RuntimeResponse, TuiDependencyError> {
        let mut stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&self.endpoint)
            .map_err(|_| TuiDependencyError::Transport)?;
        self.exchange(&mut stream, request).await
    }

    async fn exchange<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
    ) -> Result<RuntimeResponse, TuiDependencyError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (request_header, credit_windows) = self.negotiate(stream, request).await?;
        let mut current: WireFrame<RuntimeResponse> = read_frame(stream, self.maximum_frame_bytes)
            .await
            .map_err(|_| TuiDependencyError::Transport)?;
        if current.header.kind == FrameKind::Response {
            validate_response_header(&current.header, &request_header)?;
            return Ok(current.payload);
        }
        let mut expected_sequence = 1_u64;
        let mut turn_events = Vec::new();
        let mut session_events = Vec::new();
        loop {
            validate_stream_header(&current.header, &request_header, expected_sequence)?;
            match current.header.kind {
                FrameKind::StreamItem => {
                    match current.payload {
                        RuntimeResponse::TurnEvent { event, .. } => turn_events.push(event),
                        RuntimeResponse::SessionEvent {
                            event_id,
                            sequence,
                            event_type,
                            payload,
                        } => session_events.push(RuntimeSessionEvent {
                            event_id,
                            sequence,
                            event_type,
                            payload,
                        }),
                        _ => return Err(TuiDependencyError::UnexpectedResponse),
                    }
                    if credit_windows {
                        write_window_update(
                            stream,
                            &request_header,
                            expected_sequence,
                            self.maximum_frame_bytes,
                        )
                        .await?;
                    }
                }
                FrameKind::StreamEnd => {
                    return match current.payload {
                        RuntimeResponse::TurnComplete {
                            first_committed_sequence,
                            last_committed_sequence,
                            awaiting_continuation,
                        } if session_events.is_empty() => Ok(RuntimeResponse::Turn {
                            events: turn_events,
                            first_committed_sequence,
                            last_committed_sequence,
                            awaiting_continuation,
                        }),
                        RuntimeResponse::SubscriptionComplete {
                            head_sequence,
                            last_delivered_sequence,
                            has_more,
                        } if turn_events.is_empty() => Ok(RuntimeResponse::SessionEvents {
                            events: session_events,
                            head_sequence,
                            last_delivered_sequence,
                            has_more,
                        }),
                        _ => Err(TuiDependencyError::UnexpectedResponse),
                    };
                }
                _ => return Err(TuiDependencyError::Protocol),
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(TuiDependencyError::Protocol)?;
            current = read_frame(stream, self.maximum_frame_bytes)
                .await
                .map_err(|_| TuiDependencyError::Transport)?;
        }
    }

    async fn negotiate<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
    ) -> Result<(FrameHeader, bool), TuiDependencyError>
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
        .map_err(|_| TuiDependencyError::Transport)?;
        let negotiated: WireFrame<Negotiated> = read_frame(stream, self.maximum_frame_bytes)
            .await
            .map_err(|_| TuiDependencyError::Transport)?;
        validate_response_header(&negotiated.header, &handshake_header)?;
        if !negotiated
            .payload
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        {
            return Err(TuiDependencyError::Protocol);
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
        .map_err(|_| TuiDependencyError::Transport)?;
        Ok((request_header, credit_windows))
    }

    #[cfg(unix)]
    async fn stream_async(
        &self,
        request: RuntimeRequest,
        sender: mpsc::SyncSender<Result<DependencyTurnStreamItem, TuiDependencyError>>,
    ) -> Result<(), TuiDependencyError> {
        let mut stream = tokio::net::UnixStream::connect(&self.endpoint)
            .await
            .map_err(|_| TuiDependencyError::Transport)?;
        self.exchange_stream(&mut stream, request, sender).await
    }

    #[cfg(windows)]
    async fn stream_async(
        &self,
        request: RuntimeRequest,
        sender: mpsc::SyncSender<Result<DependencyTurnStreamItem, TuiDependencyError>>,
    ) -> Result<(), TuiDependencyError> {
        let mut stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&self.endpoint)
            .map_err(|_| TuiDependencyError::Transport)?;
        self.exchange_stream(&mut stream, request, sender).await
    }

    async fn exchange_stream<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
        sender: mpsc::SyncSender<Result<DependencyTurnStreamItem, TuiDependencyError>>,
    ) -> Result<(), TuiDependencyError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (request_header, credit_windows) = self.negotiate(stream, request).await?;
        let mut expected_sequence = 1_u64;
        loop {
            let frame: WireFrame<RuntimeResponse> = read_frame(stream, self.maximum_frame_bytes)
                .await
                .map_err(|_| TuiDependencyError::Transport)?;
            validate_stream_header(&frame.header, &request_header, expected_sequence)?;
            match frame.payload {
                RuntimeResponse::TurnEvent {
                    event,
                    committed_sequence,
                } if frame.header.kind == FrameKind::StreamItem => {
                    sender
                        .send(Ok(DependencyTurnStreamItem::Event {
                            event: map_turn_event(event),
                            committed_sequence,
                        }))
                        .map_err(|_| TuiDependencyError::Transport)?;
                    if credit_windows {
                        write_window_update(
                            stream,
                            &request_header,
                            expected_sequence,
                            self.maximum_frame_bytes,
                        )
                        .await?;
                    }
                }
                RuntimeResponse::TurnComplete {
                    first_committed_sequence,
                    last_committed_sequence,
                    awaiting_continuation,
                } if frame.header.kind == FrameKind::StreamEnd => {
                    sender
                        .send(Ok(DependencyTurnStreamItem::Complete {
                            first_committed_sequence,
                            last_committed_sequence,
                            awaiting_continuation,
                        }))
                        .map_err(|_| TuiDependencyError::Transport)?;
                    return Ok(());
                }
                _ => return Err(TuiDependencyError::UnexpectedResponse),
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(TuiDependencyError::Protocol)?;
        }
    }
}

impl TuiRuntimeDependencyPort for LocalRuntimeDependency {
    fn health(&self) -> Result<DependencyRuntimeHealth, TuiDependencyError> {
        let RuntimeResponse::Health { status, version } = self.send(RuntimeRequest::Health)? else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(DependencyRuntimeHealth { status, version })
    }

    fn list_styles(&self) -> Result<Vec<DependencyStyleSummary>, TuiDependencyError> {
        let RuntimeResponse::Styles { styles } = self.send(RuntimeRequest::ListStyles)? else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(styles
            .into_iter()
            .map(|style| DependencyStyleSummary {
                id: style.id,
                version: style.version,
                source: match style.source {
                    RuntimeStyleSourceKind::BuiltIn => DependencyStyleSourceKind::BuiltIn,
                    RuntimeStyleSourceKind::User => DependencyStyleSourceKind::User,
                    RuntimeStyleSourceKind::Project => DependencyStyleSourceKind::Project,
                    RuntimeStyleSourceKind::Plugin => DependencyStyleSourceKind::Plugin,
                    RuntimeStyleSourceKind::Inline => DependencyStyleSourceKind::Inline,
                },
                availability: match style.availability {
                    RuntimeStyleAvailability::Available => DependencyStyleAvailability::Available,
                    RuntimeStyleAvailability::Disabled => DependencyStyleAvailability::Disabled,
                    RuntimeStyleAvailability::Invalid => DependencyStyleAvailability::Invalid,
                    RuntimeStyleAvailability::Incompatible => {
                        DependencyStyleAvailability::Incompatible
                    }
                    RuntimeStyleAvailability::Conflict => DependencyStyleAvailability::Conflict,
                },
                style_content_hash: style.style_content_hash,
                compiled_cache_key: style.compiled_cache_key,
                required_capabilities: style.required_capabilities,
            })
            .collect())
    }

    fn list_harnesses(&self) -> Result<Vec<DependencyHarnessDescriptor>, TuiDependencyError> {
        let RuntimeResponse::Harnesses { harnesses } = self.send(RuntimeRequest::ListHarnesses)?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(harnesses
            .into_iter()
            .map(|harness| DependencyHarnessDescriptor {
                id: harness.id,
                version: harness.version,
                capabilities: harness.capabilities,
                capability_set_hash: harness.capability_set_hash,
                availability: harness.availability,
            })
            .collect())
    }

    fn list_session_components(
        &self,
    ) -> Result<DependencySessionComponentCatalog, TuiDependencyError> {
        let RuntimeResponse::SessionComponents {
            memory_providers,
            compaction_strategies,
        } = self.send(RuntimeRequest::ListSessionComponents)?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(DependencySessionComponentCatalog {
            memory_providers,
            compaction_strategies,
        })
    }

    fn list_sessions(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencySessionSummary>, TuiDependencyError> {
        let RuntimeResponse::Sessions { sessions } =
            self.send(RuntimeRequest::ListSessions { limit })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(sessions
            .into_iter()
            .map(|value| DependencySessionSummary {
                id: value.id,
                workspace: value.workspace_label,
                style: value.style,
                sequence: value.sequence,
                state: value.state,
            })
            .collect())
    }

    fn inspect_session(&self, session_id: SessionId) -> Result<Value, TuiDependencyError> {
        let RuntimeResponse::SessionInspected { state, .. } =
            self.send(RuntimeRequest::InspectSession {
                session_id,
                at: None,
            })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(state)
    }

    fn create_session(
        &self,
        workspace: String,
        style: String,
    ) -> Result<SessionId, TuiDependencyError> {
        let RuntimeResponse::SessionCreated { session_id } =
            self.send(RuntimeRequest::CreateSession {
                workspace,
                style,
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
            })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(session_id)
    }

    fn create_session_with_harness(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
    ) -> Result<SessionId, TuiDependencyError> {
        let RuntimeResponse::SessionCreated { session_id } =
            self.send(RuntimeRequest::CreateSession {
                workspace,
                style,
                harness,
                memory: None,
                compaction: None,
                budgets: None,
            })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(session_id)
    }

    fn create_session_with_components(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
        memory: Option<String>,
        compaction: Option<String>,
    ) -> Result<SessionId, TuiDependencyError> {
        let RuntimeResponse::SessionCreated { session_id } =
            self.send(RuntimeRequest::CreateSession {
                workspace,
                style,
                harness,
                memory,
                compaction,
                budgets: None,
            })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(session_id)
    }

    fn create_session_with_configuration(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<SessionId, TuiDependencyError> {
        let RuntimeResponse::SessionCreated { session_id } =
            self.send(RuntimeRequest::CreateSession {
                workspace: request.workspace,
                style: request.style,
                harness: request.harness,
                memory: request.memory,
                compaction: request.compaction,
                budgets: request
                    .budgets
                    .map(|budgets| RuntimeExecutionBudgetOverrides {
                        max_iterations: budgets.max_iterations,
                        max_steps: budgets.max_steps,
                        max_tokens: budgets.max_tokens,
                        max_cost_micros: budgets.max_cost_micros,
                        max_duration_ms: budgets.max_duration_ms,
                    }),
            })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(session_id)
    }

    fn branch_session(
        &self,
        request: DependencyBranchSessionRequest,
    ) -> Result<DependencyBranchSessionResponse, TuiDependencyError> {
        let RuntimeResponse::SessionBranched {
            session_id,
            parent_session_id,
            fork_sequence,
            child_head_sequence,
        } = self.send(RuntimeRequest::BranchSession {
            session_id: request.parent_session_id,
            at: request.at,
            style: request.style,
        })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(DependencyBranchSessionResponse {
            session_id,
            parent_session_id,
            fork_sequence,
            child_head_sequence,
        })
    }

    fn session_events(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
        limit: u32,
    ) -> Result<DependencySessionEventPage, TuiDependencyError> {
        let RuntimeResponse::SessionEvents {
            events,
            head_sequence,
            last_delivered_sequence,
            has_more,
        } = self.send(RuntimeRequest::Subscribe {
            session_id,
            after,
            limit,
        })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(DependencySessionEventPage {
            events: events
                .into_iter()
                .map(|event| DependencySessionEvent {
                    sequence: event.sequence,
                    event_type: event.event_type,
                    payload: event.payload,
                })
                .collect(),
            head_sequence,
            last_delivered_sequence,
            has_more,
        })
    }

    fn start_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        provider: String,
        model: String,
        options: Value,
        cancellation_id: CancellationId,
    ) -> Result<DependencyTurnStream, TuiDependencyError> {
        let request = RuntimeRequest::RunTurn {
            session_id,
            prompt,
            provider,
            model,
            options,
            cancellation_id,
        };
        let dependency = self.clone();
        let (sender, receiver) = mpsc::sync_channel(32);
        let errors = sender.clone();
        std::thread::Builder::new()
            .name(String::from("agentmod-tui-runtime-stream"))
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| TuiDependencyError::Transport)
                    .and_then(|runtime| runtime.block_on(dependency.stream_async(request, sender)));
                if let Err(error) = result {
                    let _ = errors.send(Err(error));
                }
            })
            .map_err(|_| TuiDependencyError::Transport)?;
        Ok(DependencyTurnStream { receiver })
    }

    fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<DependencyTurnEvent>, TuiDependencyError> {
        let RuntimeResponse::ApprovalResolved { events, .. } =
            self.send(RuntimeRequest::ResolveApproval {
                session_id,
                continuation_id,
                approved,
                resume_after_resolution: true,
            })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(events.into_iter().map(map_turn_event).collect())
    }

    fn cancel(
        &self,
        cancellation_id: CancellationId,
        reason: String,
    ) -> Result<(), TuiDependencyError> {
        let RuntimeResponse::Cancelled = self.send(RuntimeRequest::Cancel {
            cancellation_id,
            reason,
        })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(())
    }
}

fn map_turn_event(event: RuntimeProviderEvent) -> DependencyTurnEvent {
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
        RuntimeProviderEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        } => DependencyTurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        },
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
    last_received_sequence: u64,
    maximum_frame_bytes: usize,
) -> Result<(), TuiDependencyError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut header = request.clone();
    header.kind = FrameKind::WindowUpdate;
    header.stream_sequence = Some(last_received_sequence);
    write_frame(
        stream,
        &WireFrame {
            header,
            payload: RuntimeRequest::StreamWindowUpdate {
                credits: 1,
                last_received_sequence,
            },
        },
        maximum_frame_bytes,
    )
    .await
    .map_err(|_| TuiDependencyError::Transport)
}

fn validate_response_header(
    response: &FrameHeader,
    request: &FrameHeader,
) -> Result<(), TuiDependencyError> {
    if response.family != "runtime"
        || response.kind != FrameKind::Response
        || !response
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        || response.request_id != request.request_id
        || response.correlation_id != request.correlation_id
        || response.idempotency_id != request.idempotency_id
    {
        return Err(TuiDependencyError::Protocol);
    }
    Ok(())
}

fn validate_stream_header(
    response: &FrameHeader,
    request: &FrameHeader,
    expected_sequence: u64,
) -> Result<(), TuiDependencyError> {
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
        return Err(TuiDependencyError::Protocol);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TuiDependencyError {
    #[error("TUI runtime dependency configuration is invalid")]
    InvalidConfiguration,
    #[error("TUI runtime transport is unavailable")]
    Transport,
    #[error("TUI runtime protocol validation failed")]
    Protocol,
    #[error("runtime returned an unexpected TUI response")]
    UnexpectedResponse,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_is_fail_closed() {
        assert!(matches!(
            LocalRuntimeDependency::new(String::new(), String::from("short"), 0),
            Err(TuiDependencyError::InvalidConfiguration)
        ));
        assert!(
            LocalRuntimeDependency::new(
                String::from("endpoint"),
                String::from("0123456789abcdef0123456789abcdef"),
                DEFAULT_MAX_FRAME_BYTES
            )
            .is_ok()
        );
    }
}
