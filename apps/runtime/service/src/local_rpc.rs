//! Authenticated local runtime transport.
//!
//! This module owns transport lifecycle only. Every decoded runtime request is
//! passed through [`RuntimeService::handle_wire`], preserving the service to
//! logic boundary.

use std::{collections::BTreeSet, io, sync::Arc};

use agentmod_primitives::Version;
use agentmod_protocol_support::{
    DEFAULT_MAX_FRAME_BYTES, FrameHeader, FrameKind, Handshake, ProtocolError, WireFrame,
    read_frame, write_frame,
};
use agentmod_runtime_logic::{
    RuntimeLogicPort, history::SessionHistoryLogicPort, registry::SessionRegistryLogicPort,
};
use agentmod_runtime_protocol::{RuntimeRequest, RuntimeResponse};
use async_trait::async_trait;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::RuntimeService;

const INITIAL_STREAM_CREDITS: u32 = 1;
const MAX_WINDOW_CREDITS: u32 = 1_024;

/// Service-layer request dispatcher accepted by the transport.
#[async_trait]
pub trait RuntimeWireEndpoint: Send + Sync {
    /// Maps one already-authenticated runtime wire request.
    async fn handle_runtime_request(
        &self,
        request: &RuntimeRequest,
    ) -> Result<agentmod_runtime_protocol::RuntimeResponse, String>;

    /// Begins a bounded response stream for one authenticated request.
    ///
    /// Unary endpoints use the default one-frame implementation.
    async fn handle_runtime_stream(
        &self,
        request: &RuntimeRequest,
    ) -> Result<RuntimeEndpointStream, String> {
        self.handle_runtime_request(request)
            .await
            .map(RuntimeEndpointStream::single)
    }
}

/// One service-owned response frame before transport mapping.
pub struct RuntimeEndpointFrame {
    /// Endpoint response payload.
    pub response: agentmod_runtime_protocol::RuntimeResponse,
    /// Whether no later frame may follow.
    pub terminal: bool,
}

/// Bounded service-owned response stream.
pub struct RuntimeEndpointStream {
    receiver: mpsc::Receiver<Result<RuntimeEndpointFrame, String>>,
}

impl RuntimeEndpointStream {
    /// Constructs a terminal unary stream.
    #[must_use]
    pub fn single(response: agentmod_runtime_protocol::RuntimeResponse) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let _ = sender.try_send(Ok(RuntimeEndpointFrame {
            response,
            terminal: true,
        }));
        Self { receiver }
    }

    /// Wraps a bounded service-layer receiver.
    #[must_use]
    pub fn from_receiver(receiver: mpsc::Receiver<Result<RuntimeEndpointFrame, String>>) -> Self {
        Self { receiver }
    }

    /// Receives the next frame while propagating downstream backpressure.
    pub async fn next(&mut self) -> Option<Result<RuntimeEndpointFrame, String>> {
        self.receiver.recv().await
    }
}

#[async_trait]
impl<L> RuntimeWireEndpoint for RuntimeService<L>
where
    L: RuntimeLogicPort
        + SessionRegistryLogicPort
        + SessionHistoryLogicPort
        + agentmod_runtime_logic::style::SessionStyleLogicPort
        + agentmod_runtime_logic::harness_registry::HarnessRegistryLogicPort
        + Send
        + Sync,
{
    async fn handle_runtime_request(
        &self,
        request: &RuntimeRequest,
    ) -> Result<agentmod_runtime_protocol::RuntimeResponse, String> {
        self.handle_wire(request).map_err(|error| error.to_string())
    }
}

/// Runtime protocol version accepted by this service.
pub const RUNTIME_PROTOCOL_VERSION: Version = Version::new(2, 5);

/// Local endpoint configuration supplied by the composition root.
#[derive(Clone, Debug)]
pub struct LocalRpcConfig {
    /// Unix socket path or Windows named-pipe name.
    pub endpoint: String,
    /// Bootstrap secret required during the first frame.
    pub authorization_token: Arc<str>,
    /// Maximum CBOR frame body.
    pub maximum_frame_bytes: usize,
}

impl LocalRpcConfig {
    /// Validates a local RPC configuration.
    ///
    /// # Errors
    ///
    /// Returns [`LocalRpcError`] when the endpoint, secret, or frame bound is unsafe.
    pub fn validate(&self) -> Result<(), LocalRpcError> {
        if self.endpoint.trim().is_empty() {
            return Err(LocalRpcError::InvalidConfiguration(
                "local endpoint is empty",
            ));
        }
        if self.authorization_token.len() < 32 {
            return Err(LocalRpcError::InvalidConfiguration(
                "local authorization token must contain at least 32 bytes",
            ));
        }
        if self.maximum_frame_bytes == 0 || self.maximum_frame_bytes > DEFAULT_MAX_FRAME_BYTES {
            return Err(LocalRpcError::InvalidConfiguration(
                "local frame bound is outside the supported range",
            ));
        }
        Ok(())
    }
}

/// Serves one already-established local stream.
///
/// The first frame must be a handshake. Authentication is checked before any
/// runtime request is decoded or dispatched.
///
/// # Errors
///
/// Returns [`LocalRpcError`] for failed negotiation, authentication, framing,
/// or endpoint execution.
pub async fn serve_connection<S, E>(
    stream: &mut S,
    service: &E,
    config: &LocalRpcConfig,
) -> Result<(), LocalRpcError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    E: RuntimeWireEndpoint,
{
    config.validate()?;
    let handshake: WireFrame<Handshake> = read_frame(stream, config.maximum_frame_bytes).await?;
    validate_handshake_header(&handshake.header)?;
    if !constant_time_equal(
        handshake.payload.authorization_token.as_bytes(),
        config.authorization_token.as_bytes(),
    ) {
        return Err(LocalRpcError::AuthenticationFailed);
    }

    let capabilities = BTreeSet::from([
        String::from("cancellation"),
        String::from("idempotency"),
        String::from("request_response"),
        String::from("streaming"),
        String::from("bounded_backpressure"),
        String::from("credit_windows"),
    ]);
    let negotiated = agentmod_protocol_support::negotiate(
        &[RUNTIME_PROTOCOL_VERSION],
        &capabilities,
        &handshake.payload,
    )?;
    let credit_windows = negotiated.capabilities.contains("credit_windows");
    let handshake_response_header = response_header(&handshake.header, true, 1, false);
    write_frame(
        stream,
        &WireFrame {
            header: handshake_response_header,
            payload: negotiated,
        },
        config.maximum_frame_bytes,
    )
    .await?;

    loop {
        let request: WireFrame<RuntimeRequest> =
            match read_frame(stream, config.maximum_frame_bytes).await {
                Ok(frame) => frame,
                Err(ProtocolError::Io(_)) => return Ok(()),
                Err(error) => return Err(LocalRpcError::Protocol(error)),
            };
        validate_request_header(&request.header)?;
        let mut responses = service
            .handle_runtime_stream(&request.payload)
            .await
            .map_err(LocalRpcError::Endpoint)?;
        let mut stream_sequence = 0_u64;
        let mut credits = INITIAL_STREAM_CREDITS;
        while let Some(frame) = responses.next().await {
            let frame = frame.map_err(LocalRpcError::Endpoint)?;
            if credit_windows && stream_sequence > 0 && credits == 0 {
                credits = read_window_update(
                    stream,
                    &request.header,
                    stream_sequence,
                    config.maximum_frame_bytes,
                )
                .await?;
            }
            stream_sequence = stream_sequence
                .checked_add(1)
                .ok_or(LocalRpcError::StreamSequenceOverflow)?;
            let force_stream_end = matches!(
                &frame.response,
                RuntimeResponse::TurnComplete { .. } | RuntimeResponse::SubscriptionComplete { .. }
            );
            write_frame(
                stream,
                &WireFrame {
                    header: response_header(
                        &request.header,
                        frame.terminal,
                        stream_sequence,
                        force_stream_end,
                    ),
                    payload: frame.response,
                },
                config.maximum_frame_bytes,
            )
            .await?;
            if credit_windows && !frame.terminal {
                credits = credits
                    .checked_sub(1)
                    .ok_or(LocalRpcError::InvalidWindowUpdate)?;
            }
            if frame.terminal {
                break;
            }
        }
    }
}

async fn read_window_update<S>(
    stream: &mut S,
    request: &FrameHeader,
    last_sent_sequence: u64,
    maximum_frame_bytes: usize,
) -> Result<u32, LocalRpcError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let update: WireFrame<RuntimeRequest> = read_frame(stream, maximum_frame_bytes).await?;
    if update.header.family != "runtime"
        || update.header.kind != FrameKind::WindowUpdate
        || !update
            .header
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        || update.header.request_id != request.request_id
        || update.header.correlation_id != request.correlation_id
        || update.header.causation_id != request.causation_id
        || update.header.idempotency_id != request.idempotency_id
        || update.header.cancellation_id != request.cancellation_id
        || update.header.stream_sequence != Some(last_sent_sequence)
    {
        return Err(LocalRpcError::InvalidWindowUpdate);
    }
    let RuntimeRequest::StreamWindowUpdate {
        credits,
        last_received_sequence,
    } = update.payload
    else {
        return Err(LocalRpcError::InvalidWindowUpdate);
    };
    if credits == 0 || credits > MAX_WINDOW_CREDITS || last_received_sequence != last_sent_sequence
    {
        return Err(LocalRpcError::InvalidWindowUpdate);
    }
    Ok(credits)
}

/// Runs the platform-appropriate local listener.
///
/// # Errors
///
/// Returns [`LocalRpcError`] when binding, accepting, or spawning the local
/// endpoint fails. Per-connection protocol failures are isolated from the
/// listener and do not terminate the daemon.
#[cfg(unix)]
pub async fn run_local<E>(service: E, config: LocalRpcConfig) -> Result<(), LocalRpcError>
where
    E: RuntimeWireEndpoint + Clone + Send + Sync + 'static,
{
    use tokio::net::UnixListener;

    config.validate()?;
    let listener = UnixListener::bind(&config.endpoint).map_err(LocalRpcError::Bind)?;
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => Some(accepted.map_err(LocalRpcError::Accept)?),
            result = tokio::signal::ctrl_c() => {
                result.map_err(LocalRpcError::Accept)?;
                None
            }
        };
        let Some((mut stream, _)) = accepted else {
            return Ok(());
        };
        let service = service.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let _ = serve_connection(&mut stream, &service, &config).await;
        });
    }
}

/// Runs the platform-appropriate local listener.
///
/// # Errors
///
/// Returns [`LocalRpcError`] when creating or connecting a named-pipe instance
/// fails. Per-connection protocol failures are isolated from the listener.
#[cfg(windows)]
pub async fn run_local<E>(service: E, config: LocalRpcConfig) -> Result<(), LocalRpcError>
where
    E: RuntimeWireEndpoint + Clone + Send + Sync + 'static,
{
    use tokio::net::windows::named_pipe::ServerOptions;

    config.validate()?;
    loop {
        let mut server = ServerOptions::new()
            .create(&config.endpoint)
            .map_err(LocalRpcError::Bind)?;
        let connected = tokio::select! {
            result = server.connect() => {
                result.map_err(LocalRpcError::Accept)?;
                true
            }
            result = tokio::signal::ctrl_c() => {
                result.map_err(LocalRpcError::Accept)?;
                false
            }
        };
        if !connected {
            return Ok(());
        }
        let service = service.clone();
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_connection(&mut server, &service, &config).await {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "runtime.local_rpc.connection_failed",
                        "diagnostic": error.to_string(),
                    })
                );
            }
        });
    }
}

fn validate_handshake_header(header: &FrameHeader) -> Result<(), LocalRpcError> {
    if header.family != "runtime" || header.kind != FrameKind::Handshake {
        return Err(LocalRpcError::InvalidHandshake);
    }
    Ok(())
}

fn validate_request_header(header: &FrameHeader) -> Result<(), LocalRpcError> {
    if header.family != "runtime"
        || !matches!(
            header.kind,
            FrameKind::Request | FrameKind::Cancel | FrameKind::Heartbeat
        )
        || !header.version.is_compatible_with(RUNTIME_PROTOCOL_VERSION)
    {
        return Err(LocalRpcError::InvalidRequestHeader);
    }
    Ok(())
}

fn response_header(
    request: &FrameHeader,
    terminal: bool,
    stream_sequence: u64,
    force_stream_end: bool,
) -> FrameHeader {
    FrameHeader {
        family: String::from("runtime"),
        version: RUNTIME_PROTOCOL_VERSION,
        kind: if terminal {
            if stream_sequence == 1 && !force_stream_end {
                FrameKind::Response
            } else {
                FrameKind::StreamEnd
            }
        } else {
            FrameKind::StreamItem
        },
        request_id: request.request_id,
        stream_sequence: Some(stream_sequence),
        correlation_id: request.correlation_id,
        causation_id: request.causation_id,
        idempotency_id: request.idempotency_id,
        cancellation_id: request.cancellation_id,
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let maximum = left.len().max(right.len());
    for index in 0..maximum {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

/// Local transport failure.
#[derive(Debug, Error)]
pub enum LocalRpcError {
    /// Bootstrap configuration is unsafe.
    #[error("invalid local RPC configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// Listener could not be created.
    #[error("local RPC bind failed: {0}")]
    Bind(io::Error),
    /// A connection could not be accepted.
    #[error("local RPC accept failed: {0}")]
    Accept(io::Error),
    /// Bounded protocol framing or negotiation failed.
    #[error("local RPC protocol failed: {0}")]
    Protocol(#[from] ProtocolError),
    /// The first frame was not a runtime handshake.
    #[error("invalid local RPC handshake")]
    InvalidHandshake,
    /// Caller did not prove possession of the bootstrap secret.
    #[error("local RPC authentication failed")]
    AuthenticationFailed,
    /// A post-negotiation frame was not a valid runtime request.
    #[error("invalid local RPC request header")]
    InvalidRequestHeader,
    /// Service endpoint rejected the request.
    #[error("runtime endpoint failed: {0}")]
    Endpoint(String),
    /// Per-request stream sequence overflowed.
    #[error("runtime stream sequence overflow")]
    StreamSequenceOverflow,
    /// A receiver sent an invalid credit-window acknowledgement.
    #[error("invalid runtime stream window update")]
    InvalidWindowUpdate,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use agentmod_primitives::{CausationId, CorrelationId, IdempotencyId, RequestId, Sequence};
    use agentmod_protocol_support::Negotiated;
    use agentmod_runtime_logic::{
        GetRuntimeHealthCommand, LogicError, RuntimeHealthResult, RuntimeHealthState,
        registry::SessionRegistryLogicError,
    };
    use agentmod_runtime_protocol::{RuntimeProviderEvent, RuntimeResponse};
    use uuid::Uuid;

    use crate::RuntimeServiceConfig;

    use super::*;

    #[derive(Clone)]
    struct MockLogic;

    struct StreamingEndpoint {
        release_terminal: Arc<tokio::sync::Notify>,
    }

    #[derive(Clone)]
    struct CreditEndpoint;

    #[derive(Clone)]
    struct EmptyTurnEndpoint;

    #[async_trait]
    impl RuntimeWireEndpoint for EmptyTurnEndpoint {
        async fn handle_runtime_request(
            &self,
            _request: &RuntimeRequest,
        ) -> Result<RuntimeResponse, String> {
            Err(String::from("stream path required"))
        }

        async fn handle_runtime_stream(
            &self,
            _request: &RuntimeRequest,
        ) -> Result<RuntimeEndpointStream, String> {
            Ok(RuntimeEndpointStream::single(
                RuntimeResponse::TurnComplete {
                    first_committed_sequence: Sequence::FIRST,
                    last_committed_sequence: Sequence::FIRST,
                    awaiting_continuation: None,
                },
            ))
        }
    }

    #[async_trait]
    impl RuntimeWireEndpoint for StreamingEndpoint {
        async fn handle_runtime_request(
            &self,
            _request: &RuntimeRequest,
        ) -> Result<RuntimeResponse, String> {
            Err(String::from("stream path required"))
        }

        async fn handle_runtime_stream(
            &self,
            _request: &RuntimeRequest,
        ) -> Result<RuntimeEndpointStream, String> {
            let (sender, receiver) = mpsc::channel(1);
            let release = Arc::clone(&self.release_terminal);
            tokio::spawn(async move {
                let _ = sender
                    .send(Ok(RuntimeEndpointFrame {
                        response: RuntimeResponse::TurnEvent {
                            event: RuntimeProviderEvent::Started,
                            committed_sequence: Sequence::FIRST,
                        },
                        terminal: false,
                    }))
                    .await;
                release.notified().await;
                let _ = sender
                    .send(Ok(RuntimeEndpointFrame {
                        response: RuntimeResponse::TurnComplete {
                            first_committed_sequence: Sequence::FIRST,
                            last_committed_sequence: Sequence::FIRST,
                            awaiting_continuation: None,
                        },
                        terminal: true,
                    }))
                    .await;
            });
            Ok(RuntimeEndpointStream::from_receiver(receiver))
        }
    }

    #[async_trait]
    impl RuntimeWireEndpoint for CreditEndpoint {
        async fn handle_runtime_request(
            &self,
            _request: &RuntimeRequest,
        ) -> Result<RuntimeResponse, String> {
            Err(String::from("stream path required"))
        }

        async fn handle_runtime_stream(
            &self,
            _request: &RuntimeRequest,
        ) -> Result<RuntimeEndpointStream, String> {
            let (sender, receiver) = mpsc::channel(3);
            tokio::spawn(async move {
                for frame in [
                    RuntimeEndpointFrame {
                        response: RuntimeResponse::TurnEvent {
                            event: RuntimeProviderEvent::Started,
                            committed_sequence: Sequence::FIRST,
                        },
                        terminal: false,
                    },
                    RuntimeEndpointFrame {
                        response: RuntimeResponse::TurnEvent {
                            event: RuntimeProviderEvent::Text {
                                text: String::from("bounded"),
                            },
                            committed_sequence: Sequence::new(2).expect("sequence"),
                        },
                        terminal: false,
                    },
                    RuntimeEndpointFrame {
                        response: RuntimeResponse::TurnComplete {
                            first_committed_sequence: Sequence::FIRST,
                            last_committed_sequence: Sequence::new(2).expect("sequence"),
                            awaiting_continuation: None,
                        },
                        terminal: true,
                    },
                ] {
                    let _ = sender.send(Ok(frame)).await;
                }
            });
            Ok(RuntimeEndpointStream::from_receiver(receiver))
        }
    }

    impl RuntimeLogicPort for MockLogic {
        fn get_health(
            &self,
            _command: GetRuntimeHealthCommand,
        ) -> Result<RuntimeHealthResult, LogicError> {
            Ok(RuntimeHealthResult {
                state: RuntimeHealthState::Ready,
                diagnostics: vec![],
            })
        }
    }

    impl SessionRegistryLogicPort for MockLogic {
        fn create_session(
            &self,
            _command: agentmod_runtime_logic::registry::CreateSessionCommand,
        ) -> Result<agentmod_runtime_logic::registry::CreateSessionResult, SessionRegistryLogicError>
        {
            Err(SessionRegistryLogicError::InvalidWorkspace)
        }

        fn list_sessions(
            &self,
            _command: agentmod_runtime_logic::registry::ListSessionsCommand,
        ) -> Result<
            Vec<agentmod_runtime_logic::registry::SessionSummaryResult>,
            SessionRegistryLogicError,
        > {
            Ok(vec![])
        }
    }

    impl SessionHistoryLogicPort for MockLogic {
        fn inspect_session(
            &self,
            _command: agentmod_runtime_logic::history::InspectSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::InspectSessionResult,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }

        fn subscribe_session(
            &self,
            _command: agentmod_runtime_logic::history::SubscribeSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::SessionEventPage,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }

        fn branch_session(
            &self,
            _command: agentmod_runtime_logic::history::BranchSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::BranchSessionResult,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }
    }

    impl agentmod_runtime_logic::style::SessionStyleLogicPort for MockLogic {
        fn list_styles(
            &self,
            _command: agentmod_runtime_logic::style::ListStylesCommand,
        ) -> Result<
            Vec<agentmod_runtime_logic::style::StyleSummary>,
            agentmod_runtime_logic::style::SessionStyleLogicError,
        > {
            Ok(Vec::new())
        }

        fn inspect_style(
            &self,
            _command: agentmod_runtime_logic::style::InspectStyleCommand,
        ) -> Result<
            agentmod_runtime_logic::style::StyleInspection,
            agentmod_runtime_logic::style::SessionStyleLogicError,
        > {
            Err(agentmod_runtime_logic::style::SessionStyleLogicError::InvalidSelector)
        }

        fn validate_style(
            &self,
            _command: agentmod_runtime_logic::style::ValidateStyleCommand,
        ) -> Result<
            agentmod_runtime_logic::style::StyleInspection,
            agentmod_runtime_logic::style::SessionStyleLogicError,
        > {
            Err(agentmod_runtime_logic::style::SessionStyleLogicError::EmptyManifest)
        }

        fn resolve_style(
            &self,
            _command: agentmod_runtime_logic::style::InspectStyleCommand,
        ) -> Result<
            agentmod_runtime_logic::style::ResolvedStyle,
            agentmod_runtime_logic::style::SessionStyleLogicError,
        > {
            Err(agentmod_runtime_logic::style::SessionStyleLogicError::InvalidSelector)
        }

        fn validate_style_binding(
            &self,
            _command: agentmod_runtime_logic::style::ValidateStyleBindingCommand,
        ) -> Result<(), agentmod_runtime_logic::style::SessionStyleLogicError> {
            Err(agentmod_runtime_logic::style::SessionStyleLogicError::InvalidSelector)
        }
    }

    impl agentmod_runtime_logic::harness_registry::HarnessRegistryLogicPort for MockLogic {
        fn list_harnesses(
            &self,
        ) -> Result<
            Vec<agentmod_runtime_logic::harness_registry::HarnessDescriptor>,
            agentmod_runtime_logic::harness_registry::HarnessRegistryLogicError,
        > {
            Ok(Vec::new())
        }

        fn inspect_harness(
            &self,
            id: &str,
        ) -> Result<
            agentmod_runtime_logic::harness_registry::HarnessDescriptor,
            agentmod_runtime_logic::harness_registry::HarnessRegistryLogicError,
        > {
            Err(
                agentmod_runtime_logic::harness_registry::HarnessRegistryLogicError::NotFound(
                    id.to_owned(),
                ),
            )
        }
    }

    fn config(token: &str) -> LocalRpcConfig {
        LocalRpcConfig {
            endpoint: String::from("fixture"),
            authorization_token: Arc::from(token),
            maximum_frame_bytes: 4096,
        }
    }

    fn header(kind: FrameKind, suffix: u128) -> FrameHeader {
        FrameHeader {
            family: String::from("runtime"),
            version: RUNTIME_PROTOCOL_VERSION,
            kind,
            request_id: RequestId::from_uuid(Uuid::from_u128(suffix)),
            stream_sequence: None,
            correlation_id: CorrelationId::from_uuid(Uuid::from_u128(suffix + 1)),
            causation_id: CausationId::from_uuid(Uuid::from_u128(suffix + 2)),
            idempotency_id: IdempotencyId::from_uuid(Uuid::from_u128(suffix + 3)),
            cancellation_id: None,
        }
    }

    fn service() -> RuntimeService<MockLogic> {
        RuntimeService::new(
            MockLogic,
            RuntimeServiceConfig {
                session_root: PathBuf::from("sessions"),
                version: String::from("test"),
                styles: crate::RuntimeStyleServiceConfig::native(std::path::Path::new("sessions")),
            },
        )
    }

    #[tokio::test]
    async fn authenticates_negotiates_and_dispatches_health() {
        let token = "0123456789abcdef0123456789abcdef";
        let (mut client, mut server) = tokio::io::duplex(16 * 1024);
        let server_task =
            tokio::spawn(
                async move { serve_connection(&mut server, &service(), &config(token)).await },
            );
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Handshake, 10),
                payload: Handshake {
                    supported_versions: vec![RUNTIME_PROTOCOL_VERSION],
                    capabilities: BTreeSet::from([String::from("request_response")]),
                    authorization_token: String::from(token),
                },
            },
            4096,
        )
        .await
        .expect("handshake");
        let negotiated: WireFrame<Negotiated> =
            read_frame(&mut client, 4096).await.expect("negotiated");
        assert_eq!(negotiated.payload.version, RUNTIME_PROTOCOL_VERSION);

        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Request, 20),
                payload: RuntimeRequest::Health,
            },
            4096,
        )
        .await
        .expect("request");
        let response: WireFrame<RuntimeResponse> =
            read_frame(&mut client, 4096).await.expect("response");
        assert_eq!(
            response.payload,
            RuntimeResponse::Health {
                status: String::from("ok"),
                version: String::from("test")
            }
        );
        drop(client);
        server_task.await.expect("task").expect("connection");
    }

    #[tokio::test]
    async fn zero_event_turn_uses_stream_end_even_as_the_first_frame() {
        let token = "0123456789abcdef0123456789abcdef";
        let (mut client, mut server) = tokio::io::duplex(16 * 1024);
        let server_task = tokio::spawn(async move {
            serve_connection(&mut server, &EmptyTurnEndpoint, &config(token)).await
        });
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Handshake, 30),
                payload: Handshake {
                    supported_versions: vec![RUNTIME_PROTOCOL_VERSION],
                    capabilities: BTreeSet::from([String::from("streaming")]),
                    authorization_token: String::from(token),
                },
            },
            4096,
        )
        .await
        .expect("handshake");
        let _: WireFrame<Negotiated> = read_frame(&mut client, 4096).await.expect("negotiated");
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Request, 31),
                payload: RuntimeRequest::Health,
            },
            4096,
        )
        .await
        .expect("request");

        let terminal: WireFrame<RuntimeResponse> =
            read_frame(&mut client, 4096).await.expect("terminal");
        assert_eq!(terminal.header.kind, FrameKind::StreamEnd);
        assert_eq!(terminal.header.stream_sequence, Some(1));
        assert!(matches!(
            terminal.payload,
            RuntimeResponse::TurnComplete { .. }
        ));
        drop(client);
        server_task.await.expect("task").expect("connection");
    }

    #[tokio::test]
    async fn delivers_bounded_stream_item_before_terminal_frame() {
        let token = "0123456789abcdef0123456789abcdef";
        let release_terminal = Arc::new(tokio::sync::Notify::new());
        let endpoint = StreamingEndpoint {
            release_terminal: Arc::clone(&release_terminal),
        };
        let (mut client, mut server) = tokio::io::duplex(16 * 1024);
        let server_task =
            tokio::spawn(
                async move { serve_connection(&mut server, &endpoint, &config(token)).await },
            );
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Handshake, 40),
                payload: Handshake {
                    supported_versions: vec![RUNTIME_PROTOCOL_VERSION],
                    capabilities: BTreeSet::from([String::from("streaming")]),
                    authorization_token: String::from(token),
                },
            },
            4096,
        )
        .await
        .expect("handshake");
        let _: WireFrame<Negotiated> = read_frame(&mut client, 4096).await.expect("negotiated");
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Request, 50),
                payload: RuntimeRequest::Health,
            },
            4096,
        )
        .await
        .expect("request");

        let first: WireFrame<RuntimeResponse> = read_frame(&mut client, 4096)
            .await
            .expect("first stream item");
        assert_eq!(first.header.kind, FrameKind::StreamItem);
        assert_eq!(first.header.stream_sequence, Some(1));
        assert!(matches!(
            first.payload,
            RuntimeResponse::TurnEvent {
                event: RuntimeProviderEvent::Started,
                committed_sequence: Sequence::FIRST,
            }
        ));
        release_terminal.notify_one();
        let terminal: WireFrame<RuntimeResponse> = read_frame(&mut client, 4096)
            .await
            .expect("stream terminal");
        assert_eq!(terminal.header.kind, FrameKind::StreamEnd);
        assert_eq!(terminal.header.stream_sequence, Some(2));
        assert!(matches!(
            terminal.payload,
            RuntimeResponse::TurnComplete { .. }
        ));
        drop(client);
        server_task.await.expect("task").expect("connection");
    }

    #[tokio::test]
    async fn credit_window_blocks_second_item_until_valid_acknowledgement() {
        let token = "0123456789abcdef0123456789abcdef";
        let (mut client, mut server) = tokio::io::duplex(16 * 1024);
        let server_task = tokio::spawn(async move {
            serve_connection(&mut server, &CreditEndpoint, &config(token)).await
        });
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Handshake, 70),
                payload: Handshake {
                    supported_versions: vec![RUNTIME_PROTOCOL_VERSION],
                    capabilities: BTreeSet::from([
                        String::from("streaming"),
                        String::from("credit_windows"),
                    ]),
                    authorization_token: String::from(token),
                },
            },
            4096,
        )
        .await
        .expect("handshake");
        let negotiated: WireFrame<Negotiated> =
            read_frame(&mut client, 4096).await.expect("negotiated");
        assert!(negotiated.payload.capabilities.contains("credit_windows"));

        let request_header = header(FrameKind::Request, 80);
        write_frame(
            &mut client,
            &WireFrame {
                header: request_header.clone(),
                payload: RuntimeRequest::Health,
            },
            4096,
        )
        .await
        .expect("request");
        let first: WireFrame<RuntimeResponse> =
            read_frame(&mut client, 4096).await.expect("first item");
        assert_eq!(first.header.stream_sequence, Some(1));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                read_frame::<_, RuntimeResponse>(&mut client, 4096),
            )
            .await
            .is_err(),
            "server sent beyond the one-item credit window"
        );

        let mut update_header = request_header.clone();
        update_header.kind = FrameKind::WindowUpdate;
        update_header.stream_sequence = Some(1);
        write_frame(
            &mut client,
            &WireFrame {
                header: update_header,
                payload: RuntimeRequest::StreamWindowUpdate {
                    credits: 1,
                    last_received_sequence: 1,
                },
            },
            4096,
        )
        .await
        .expect("window update");
        let second: WireFrame<RuntimeResponse> =
            read_frame(&mut client, 4096).await.expect("second item");
        assert_eq!(second.header.stream_sequence, Some(2));
        let mut final_update_header = request_header;
        final_update_header.kind = FrameKind::WindowUpdate;
        final_update_header.stream_sequence = Some(2);
        write_frame(
            &mut client,
            &WireFrame {
                header: final_update_header,
                payload: RuntimeRequest::StreamWindowUpdate {
                    credits: 1,
                    last_received_sequence: 2,
                },
            },
            4096,
        )
        .await
        .expect("final window update");
        let terminal: WireFrame<RuntimeResponse> =
            read_frame(&mut client, 4096).await.expect("terminal");
        assert_eq!(terminal.header.kind, FrameKind::StreamEnd);
        drop(client);
        server_task.await.expect("task").expect("connection");
    }

    #[tokio::test]
    async fn rejects_wrong_secret_before_dispatch() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            serve_connection(
                &mut server,
                &service(),
                &config("0123456789abcdef0123456789abcdef"),
            )
            .await
        });
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Handshake, 30),
                payload: Handshake {
                    supported_versions: vec![RUNTIME_PROTOCOL_VERSION],
                    capabilities: BTreeSet::new(),
                    authorization_token: String::from("wrong-wrong-wrong-wrong-wrong-wrong"),
                },
            },
            4096,
        )
        .await
        .expect("handshake");
        assert!(matches!(
            server_task.await.expect("task"),
            Err(LocalRpcError::AuthenticationFailed)
        ));
    }

    #[test]
    fn configuration_rejects_short_secrets_and_excessive_frames() {
        assert!(matches!(
            config("short").validate(),
            Err(LocalRpcError::InvalidConfiguration(_))
        ));
        let mut excessive = config("0123456789abcdef0123456789abcdef");
        excessive.maximum_frame_bytes = DEFAULT_MAX_FRAME_BYTES + 1;
        assert!(matches!(
            excessive.validate(),
            Err(LocalRpcError::InvalidConfiguration(_))
        ));
    }
}
