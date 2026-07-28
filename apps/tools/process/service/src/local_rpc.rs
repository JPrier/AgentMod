//! Authenticated reconnectable process-host transport.
//!
//! Transport DTOs are decoded here and mapped into the existing service
//! endpoint. Neither process logic nor process data imports protocol framing.

use std::{
    collections::BTreeSet,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use agentmod_process_host_logic::ProcessLogicPort;
use agentmod_protocol_support::{
    FrameHeader, FrameKind, Handshake, ProtocolError, WireFrame, read_frame, write_frame,
};
use agentmod_tool_protocol::{PROTOCOL_VERSION, ToolHostCommand, ToolHostEvent};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{ProcessHostService, ProcessServiceError};

/// Local process-host endpoint configuration.
#[derive(Clone, Debug)]
pub struct ProcessLocalRpcConfig {
    /// Unix socket path or Windows named-pipe name.
    pub endpoint: String,
    /// Bootstrap secret proven before any command is decoded.
    pub authorization_token: Arc<str>,
    /// Maximum CBOR frame body.
    pub maximum_frame_bytes: usize,
    /// Period without a client before checking whether no live child handles
    /// remain and the host may exit.
    pub idle_check_interval: Duration,
}

impl ProcessLocalRpcConfig {
    /// Validates endpoint bootstrap limits.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessLocalRpcError::InvalidConfiguration`] for unsafe
    /// endpoint, authentication, or frame settings.
    pub fn validate(&self) -> Result<(), ProcessLocalRpcError> {
        if self.endpoint.trim().is_empty() {
            return Err(ProcessLocalRpcError::InvalidConfiguration(
                "local endpoint is empty",
            ));
        }
        if self.authorization_token.len() < 32 {
            return Err(ProcessLocalRpcError::InvalidConfiguration(
                "local authorization token must contain at least 32 bytes",
            ));
        }
        if self.maximum_frame_bytes == 0
            || self.maximum_frame_bytes > agentmod_protocol_support::DEFAULT_MAX_FRAME_BYTES
        {
            return Err(ProcessLocalRpcError::InvalidConfiguration(
                "local frame bound is outside the supported range",
            ));
        }
        if self.idle_check_interval.is_zero()
            || self.idle_check_interval > Duration::from_secs(24 * 60 * 60)
        {
            return Err(ProcessLocalRpcError::InvalidConfiguration(
                "local idle interval is outside the supported range",
            ));
        }
        Ok(())
    }
}

/// Serves one authenticated reconnectable stream.
///
/// # Errors
///
/// Returns [`ProcessLocalRpcError`] for failed authentication, negotiation,
/// framing, or service execution.
pub async fn serve_connection<S, L>(
    stream: &mut S,
    service: &ProcessHostService<L>,
    config: &ProcessLocalRpcConfig,
) -> Result<(), ProcessLocalRpcError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    L: ProcessLogicPort,
{
    serve_connection_inner(stream, service, config, None).await
}

async fn serve_connection_inner<S, L>(
    stream: &mut S,
    service: &ProcessHostService<L>,
    config: &ProcessLocalRpcConfig,
    active_requests: Option<Arc<AtomicUsize>>,
) -> Result<(), ProcessLocalRpcError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    L: ProcessLogicPort,
{
    config.validate()?;
    let handshake_activity = active_requests
        .as_ref()
        .map(|count| ActiveRequest::new(Arc::clone(count)));
    let handshake: WireFrame<Handshake> = read_frame(stream, config.maximum_frame_bytes).await?;
    if handshake.header.family != "tool" || handshake.header.kind != FrameKind::Handshake {
        return Err(ProcessLocalRpcError::InvalidHandshake);
    }
    if !constant_time_equal(
        handshake.payload.authorization_token.as_bytes(),
        config.authorization_token.as_bytes(),
    ) {
        return Err(ProcessLocalRpcError::AuthenticationFailed);
    }
    let capabilities = BTreeSet::from([
        String::from("bounded_backpressure"),
        String::from("cancellation"),
        String::from("idempotency"),
        String::from("request_response"),
        String::from("streaming"),
    ]);
    let negotiated = agentmod_protocol_support::negotiate(
        &[PROTOCOL_VERSION],
        &capabilities,
        &handshake.payload,
    )?;
    write_frame(
        stream,
        &WireFrame {
            header: response_header(&handshake.header, FrameKind::Response, Some(1)),
            payload: negotiated,
        },
        config.maximum_frame_bytes,
    )
    .await?;
    drop(handshake_activity);

    loop {
        let request: WireFrame<ToolHostCommand> =
            match read_frame(stream, config.maximum_frame_bytes).await {
                Ok(frame) => frame,
                Err(ProtocolError::Io(_)) => return Ok(()),
                Err(error) => return Err(ProcessLocalRpcError::Protocol(error)),
            };
        validate_request_header(&request.header)?;
        let _activity = active_requests
            .as_ref()
            .map(|count| ActiveRequest::new(Arc::clone(count)));
        let call_id = command_call_id(&request.payload);
        let events = match service.handle(request.payload).await {
            Ok(events) => events,
            Err(error) => vec![ToolHostEvent::Failed {
                call_id,
                code: endpoint_error_code(&error).to_owned(),
                message: String::from("process request was rejected"),
                retryable: false,
            }],
        };
        if events.is_empty() {
            return Err(ProcessLocalRpcError::EmptyResponse);
        }
        let event_count = events.len();
        for (index, event) in events.into_iter().enumerate() {
            let sequence =
                u64::try_from(index + 1).map_err(|_| ProcessLocalRpcError::SequenceOverflow)?;
            let terminal = index + 1 == event_count;
            let kind = if terminal {
                if sequence == 1 {
                    FrameKind::Response
                } else {
                    FrameKind::StreamEnd
                }
            } else {
                FrameKind::StreamItem
            };
            write_frame(
                stream,
                &WireFrame {
                    header: response_header(&request.header, kind, Some(sequence)),
                    payload: event,
                },
                config.maximum_frame_bytes,
            )
            .await?;
        }
    }
}

/// Runs the Unix socket listener until process shutdown.
///
/// Individual connection failures are isolated from the host.
///
/// # Errors
///
/// Returns [`ProcessLocalRpcError`] when configuration, bind, accept, or
/// shutdown-signal handling fails.
#[cfg(unix)]
pub async fn run_local<L>(
    service: ProcessHostService<L>,
    config: ProcessLocalRpcConfig,
) -> Result<(), ProcessLocalRpcError>
where
    L: ProcessLogicPort + Clone + Send + Sync + 'static,
{
    use tokio::net::UnixListener;

    config.validate()?;
    let listener = UnixListener::bind(&config.endpoint).map_err(ProcessLocalRpcError::Bind)?;
    let active_requests = Arc::new(AtomicUsize::new(0));
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => Some(Some(accepted.map_err(ProcessLocalRpcError::Accept)?)),
            result = tokio::signal::ctrl_c() => {
                result.map_err(ProcessLocalRpcError::Accept)?;
                None
            }
            () = tokio::time::sleep(config.idle_check_interval) => Some(None)
        };
        let Some(accepted) = accepted else {
            return Ok(());
        };
        let Some((mut stream, _)) = accepted else {
            if active_requests.load(Ordering::Acquire) == 0
                && service
                    .may_exit_idle()
                    .await
                    .map_err(|_| ProcessLocalRpcError::Endpoint)?
            {
                return Ok(());
            }
            continue;
        };
        let service = service.clone();
        let config = config.clone();
        let active_requests = Arc::clone(&active_requests);
        tokio::spawn(async move {
            let _ =
                serve_connection_inner(&mut stream, &service, &config, Some(active_requests)).await;
        });
    }
}

/// Runs the Windows named-pipe listener until process shutdown.
///
/// Remote clients are rejected by the pipe implementation and each connection
/// still proves the bootstrap secret.
///
/// # Errors
///
/// Returns [`ProcessLocalRpcError`] when configuration, pipe creation,
/// connection, or shutdown-signal handling fails.
#[cfg(windows)]
pub async fn run_local<L>(
    service: ProcessHostService<L>,
    config: ProcessLocalRpcConfig,
) -> Result<(), ProcessLocalRpcError>
where
    L: ProcessLogicPort + Clone + Send + Sync + 'static,
{
    use tokio::net::windows::named_pipe::ServerOptions;

    config.validate()?;
    let mut first = true;
    let active_requests = Arc::new(AtomicUsize::new(0));
    loop {
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first)
            .reject_remote_clients(true);
        let mut server = options
            .create(&config.endpoint)
            .map_err(ProcessLocalRpcError::Bind)?;
        first = false;
        let connected = tokio::select! {
            result = server.connect() => {
                result.map_err(ProcessLocalRpcError::Accept)?;
                Some(true)
            }
            result = tokio::signal::ctrl_c() => {
                result.map_err(ProcessLocalRpcError::Accept)?;
                None
            }
            () = tokio::time::sleep(config.idle_check_interval) => Some(false)
        };
        let Some(connected) = connected else {
            return Ok(());
        };
        if !connected {
            let active = active_requests.load(Ordering::Acquire);
            let may_exit = service
                .may_exit_idle()
                .await
                .map_err(|_| ProcessLocalRpcError::Endpoint)?;
            if active == 0 && may_exit {
                return Ok(());
            }
            continue;
        }
        let service = service.clone();
        let config = config.clone();
        let active_requests = Arc::clone(&active_requests);
        tokio::spawn(async move {
            let _ =
                serve_connection_inner(&mut server, &service, &config, Some(active_requests)).await;
        });
    }
}

struct ActiveRequest {
    count: Arc<AtomicUsize>,
}

impl ActiveRequest {
    fn new(count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self { count }
    }
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

fn validate_request_header(header: &FrameHeader) -> Result<(), ProcessLocalRpcError> {
    if header.family != "tool"
        || !matches!(
            header.kind,
            FrameKind::Request | FrameKind::Cancel | FrameKind::Heartbeat
        )
        || !header.version.is_compatible_with(PROTOCOL_VERSION)
        || header.stream_sequence.is_some()
    {
        return Err(ProcessLocalRpcError::InvalidRequestHeader);
    }
    Ok(())
}

fn command_call_id(command: &ToolHostCommand) -> String {
    match command {
        ToolHostCommand::Execute { call_id, .. } => call_id.clone(),
        ToolHostCommand::Cancel { cancellation_id } => cancellation_id.to_string(),
        ToolHostCommand::DiscoverGroups => String::from("discover-groups"),
        ToolHostCommand::DiscoverTools { .. } => String::from("discover-tools"),
        ToolHostCommand::Health => String::from("health"),
    }
}

const fn endpoint_error_code(error: &ProcessServiceError) -> &'static str {
    match error {
        ProcessServiceError::MissingConfiguration => "host_misconfigured",
        ProcessServiceError::InvalidAuthorizationEnvelope => "authorization_invalid",
        ProcessServiceError::UnknownTool => "unknown_tool",
        ProcessServiceError::InvalidArguments => "invalid_arguments",
        ProcessServiceError::Authorization => "authorization_denied",
        ProcessServiceError::Logic => "operation_rejected",
    }
}

fn response_header(
    request: &FrameHeader,
    kind: FrameKind,
    stream_sequence: Option<u64>,
) -> FrameHeader {
    FrameHeader {
        family: String::from("tool"),
        version: PROTOCOL_VERSION,
        kind,
        request_id: request.request_id,
        stream_sequence,
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
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

/// Reconnectable process-host transport failure.
#[derive(Debug, Error)]
pub enum ProcessLocalRpcError {
    /// Bootstrap configuration is unsafe.
    #[error("invalid local process RPC configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// Listener could not be created.
    #[error("local process RPC bind failed: {0}")]
    Bind(io::Error),
    /// A connection could not be accepted.
    #[error("local process RPC accept failed: {0}")]
    Accept(io::Error),
    /// Bounded protocol framing or negotiation failed.
    #[error("local process RPC protocol failed: {0}")]
    Protocol(#[from] ProtocolError),
    /// The first frame was not a tool handshake.
    #[error("invalid local process RPC handshake")]
    InvalidHandshake,
    /// Caller did not prove the bootstrap secret.
    #[error("local process RPC authentication failed")]
    AuthenticationFailed,
    /// A post-negotiation frame was not a valid tool request.
    #[error("invalid local process RPC request header")]
    InvalidRequestHeader,
    /// Service endpoint rejected the command.
    #[error("process service rejected the command")]
    Endpoint,
    /// Service produced no response frame.
    #[error("process service produced an empty response")]
    EmptyResponse,
    /// Per-request stream sequence overflowed.
    #[error("process response sequence overflow")]
    SequenceOverflow,
}

#[cfg(test)]
mod tests {
    use agentmod_primitives::{CausationId, CorrelationId, IdempotencyId, RequestId};
    use agentmod_process_host_logic::{
        CancelProcessCommand, ProcessAuthorization, ProcessLogicError, ProcessLogicPort,
        ProcessResult, ReadOutputQuery, ResizeTerminalCommand, StartProcessCommand,
    };
    use agentmod_protocol_support::{Negotiated, write_frame};
    use async_trait::async_trait;
    use tokio::io::duplex;
    use uuid::Uuid;

    use crate::ProcessHostServiceConfig;

    use super::*;

    #[derive(Clone)]
    struct MockLogic;

    #[async_trait]
    impl ProcessLogicPort for MockLogic {
        async fn start(
            &self,
            _command: StartProcessCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn read_output(
            &self,
            _query: ReadOutputQuery,
        ) -> Result<agentmod_process_host_logic::OutputRange, ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn input(
            &self,
            _command: agentmod_process_host_logic::InputProcessCommand,
        ) -> Result<(), ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn resize(
            &self,
            _command: ResizeTerminalCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn wait(
            &self,
            _command: agentmod_process_host_logic::ProcessControlCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn interrupt(
            &self,
            _command: agentmod_process_host_logic::ProcessControlCommand,
        ) -> Result<(), ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn kill(
            &self,
            _command: agentmod_process_host_logic::ProcessControlCommand,
        ) -> Result<(), ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn detach(
            &self,
            _command: agentmod_process_host_logic::ProcessControlCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn reattach(
            &self,
            _command: agentmod_process_host_logic::ProcessControlCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            Err(ProcessLogicError::Operation)
        }

        async fn list(
            &self,
            _authorization: ProcessAuthorization,
        ) -> Result<Vec<ProcessResult>, ProcessLogicError> {
            Ok(Vec::new())
        }

        async fn active_count(
            &self,
            _identity: agentmod_process_host_logic::ProcessIdentity,
        ) -> Result<usize, ProcessLogicError> {
            Ok(0)
        }

        async fn cancel(
            &self,
            _command: CancelProcessCommand,
        ) -> Result<String, ProcessLogicError> {
            Ok(String::from("cancelled-call"))
        }
    }

    fn header(kind: FrameKind) -> FrameHeader {
        FrameHeader {
            family: String::from("tool"),
            version: PROTOCOL_VERSION,
            kind,
            request_id: RequestId::from_uuid(Uuid::now_v7()),
            stream_sequence: None,
            correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
            causation_id: CausationId::from_uuid(Uuid::now_v7()),
            idempotency_id: IdempotencyId::from_uuid(Uuid::now_v7()),
            cancellation_id: None,
        }
    }

    #[tokio::test]
    async fn authenticates_negotiates_and_streams_service_events() {
        let service = ProcessHostService::new(
            MockLogic,
            ProcessHostServiceConfig {
                owner_id: String::from("owner"),
                session_id: String::from("session"),
            },
        )
        .expect("service");
        let config = ProcessLocalRpcConfig {
            endpoint: String::from("test"),
            authorization_token: Arc::from("a".repeat(32)),
            maximum_frame_bytes: 1024 * 1024,
            idle_check_interval: Duration::from_secs(30),
        };
        let server_config = config.clone();
        let (mut client, mut server) = duplex(64 * 1024);
        let task =
            tokio::spawn(
                async move { serve_connection(&mut server, &service, &server_config).await },
            );

        let handshake_header = header(FrameKind::Handshake);
        write_frame(
            &mut client,
            &WireFrame {
                header: handshake_header.clone(),
                payload: Handshake {
                    supported_versions: vec![PROTOCOL_VERSION],
                    capabilities: BTreeSet::from([
                        String::from("request_response"),
                        String::from("streaming"),
                    ]),
                    authorization_token: "a".repeat(32),
                },
            },
            config.maximum_frame_bytes,
        )
        .await
        .expect("handshake");
        let negotiated: WireFrame<Negotiated> = read_frame(&mut client, config.maximum_frame_bytes)
            .await
            .expect("negotiated");
        assert_eq!(negotiated.header.request_id, handshake_header.request_id);
        assert_eq!(negotiated.payload.version, PROTOCOL_VERSION);

        let request_header = header(FrameKind::Request);
        write_frame(
            &mut client,
            &WireFrame {
                header: request_header.clone(),
                payload: ToolHostCommand::Health,
            },
            config.maximum_frame_bytes,
        )
        .await
        .expect("health");
        let response: WireFrame<ToolHostEvent> =
            read_frame(&mut client, config.maximum_frame_bytes)
                .await
                .expect("response");
        assert_eq!(response.header.kind, FrameKind::Response);
        assert_eq!(response.header.request_id, request_header.request_id);
        assert!(matches!(
            response.payload,
            ToolHostEvent::Progress {
                completed: Some(1),
                total: Some(1),
                ..
            }
        ));
        drop(client);
        task.await.expect("join").expect("server");
    }

    #[tokio::test]
    async fn rejects_wrong_bootstrap_secret_before_dispatch() {
        let service = ProcessHostService::new(
            MockLogic,
            ProcessHostServiceConfig {
                owner_id: String::from("owner"),
                session_id: String::from("session"),
            },
        )
        .expect("service");
        let config = ProcessLocalRpcConfig {
            endpoint: String::from("test"),
            authorization_token: Arc::from("a".repeat(32)),
            maximum_frame_bytes: 1024 * 1024,
            idle_check_interval: Duration::from_secs(30),
        };
        let (mut client, mut server) = duplex(64 * 1024);
        let task =
            tokio::spawn(async move { serve_connection(&mut server, &service, &config).await });
        write_frame(
            &mut client,
            &WireFrame {
                header: header(FrameKind::Handshake),
                payload: Handshake {
                    supported_versions: vec![PROTOCOL_VERSION],
                    capabilities: BTreeSet::new(),
                    authorization_token: "b".repeat(32),
                },
            },
            1024 * 1024,
        )
        .await
        .expect("handshake");
        assert!(matches!(
            task.await.expect("join"),
            Err(ProcessLocalRpcError::AuthenticationFailed)
        ));
    }

    #[test]
    fn configuration_is_bounded() {
        let config = ProcessLocalRpcConfig {
            endpoint: String::new(),
            authorization_token: Arc::from("short"),
            maximum_frame_bytes: 0,
            idle_check_interval: Duration::ZERO,
        };
        assert!(matches!(
            config.validate(),
            Err(ProcessLocalRpcError::InvalidConfiguration(_))
        ));
    }
}
