//! CLI-owned adapters for communicating with the runtime boundary.
#![allow(
    missing_docs,
    reason = "dependency-local turn records are boundary-specific"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the CLI dependency port uses one documented closed error taxonomy"
)]

use std::{
    collections::BTreeSet,
    path::Path,
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
    RuntimeProviderEvent, RuntimeRequest, RuntimeResponse, RuntimeSchedulePayload,
    RuntimeScheduleSpec, RuntimeScheduleTrigger, RuntimeSessionEvent, RuntimeStyleAvailability,
    RuntimeStyleDiagnostic, RuntimeStyleInspection, RuntimeStyleManifestFormat,
    RuntimeStyleSourceKind, RuntimeStyleSummary,
};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

const RUNTIME_PROTOCOL_VERSION: Version = Version::new(2, 1);
const MAX_STYLE_MANIFEST_BYTES: u64 = 1_048_576;

/// Dependency-owned style manifest format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyStyleManifestFormat {
    Toml,
    Json,
}

/// Dependency-owned style source kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyStyleSourceKind {
    BuiltIn,
    User,
    Project,
    Plugin,
    Inline,
}

/// Dependency-owned style availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyStyleAvailability {
    Available,
    Disabled,
    Invalid,
    Incompatible,
    Conflict,
}

/// Dependency-owned style diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleDiagnostic {
    pub code: String,
    pub path: String,
    pub message: String,
    pub help: String,
}

/// Dependency-owned style summary.
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

/// Dependency-owned detailed style inspection.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyStyleInspection {
    pub summary: DependencyStyleSummary,
    pub source_locator: String,
    pub manifest: Value,
    pub compiled: Option<Value>,
    pub diagnostics: Vec<DependencyStyleDiagnostic>,
}

/// Dependency-owned selector for a registry entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyInspectStyleRequest {
    pub selector: String,
}

/// Dependency-owned style manifest file request. Reading is contained here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleFileRequest {
    pub file: String,
}

/// Dependency-owned validation result; invalid manifests are normal results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleValidationResult {
    pub valid: bool,
    pub diagnostics: Vec<DependencyStyleDiagnostic>,
}

/// Dependency-owned runtime health request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRuntimeHealthRequest {
    /// Stable client label used for dependency diagnostics.
    pub client_label: String,
}

/// Dependency-owned normalized runtime health response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRuntimeHealthResponse {
    /// Runtime availability as understood by the outbound adapter.
    pub availability: DependencyRuntimeAvailability,
    /// Runtime build version returned over the wire.
    pub runtime_version: String,
}

/// Dependency-owned create-session request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateSessionRequest {
    /// Workspace text.
    pub workspace: String,
    /// Explicit style.
    pub style: String,
}

/// Dependency-owned create-session response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateSessionResponse {
    /// Runtime session ID.
    pub session_id: SessionId,
}

/// Dependency-owned list request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyListSessionsRequest {
    /// Maximum rows.
    pub limit: u32,
}

/// Dependency-owned session row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySessionSummary {
    /// Session ID.
    pub id: SessionId,
    /// Safe workspace label.
    pub workspace_label: String,
    /// Explicit style.
    pub style: String,
    /// Last sequence.
    pub sequence: Sequence,
    /// Lifecycle label.
    pub state: String,
}

/// Dependency-owned point-in-time request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyInspectSessionRequest {
    pub session_id: SessionId,
    pub at: Option<Sequence>,
    pub replay: bool,
}

/// Dependency-owned replay result.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyInspectSessionResponse {
    pub session_id: SessionId,
    pub head_sequence: Sequence,
    pub inspected_sequence: Sequence,
    pub event_count: u64,
    pub state: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySubscribeSessionRequest {
    pub session_id: SessionId,
    pub after: Option<Sequence>,
    pub limit: u32,
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

/// Dependency-owned branch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyBranchSessionRequest {
    pub session_id: SessionId,
    pub at: Sequence,
    pub style: Option<String>,
}

/// Dependency-owned branch result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyBranchSessionResponse {
    pub session_id: SessionId,
    pub parent_session_id: SessionId,
    pub fork_sequence: Sequence,
    pub child_head_sequence: Sequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyScheduleTrigger {
    AtMillis(i64),
    Interval {
        starts_at_ms: i64,
        every_ms: u64,
    },
    RuntimeEvent {
        event_type: String,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencySchedulePayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySchedule {
    pub schedule_id: String,
    pub session_id: SessionId,
    pub idempotency_id: String,
    pub style: String,
    pub workspace: String,
    pub permission_policy: String,
    pub provider: String,
    pub model: String,
    pub token_budget: u64,
    pub cost_budget_micros: u64,
    pub trigger: DependencyScheduleTrigger,
    pub payload: DependencySchedulePayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScheduledExecution {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub claimed_at_ms: i64,
    pub schedule: DependencySchedule,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScheduleStoreResult {
    pub schedule_id: String,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyCreateDeferredTurnRequest {
    pub session_id: SessionId,
    pub continuation_id: String,
    pub schedule_id: String,
    pub prompt: String,
    pub workspace: String,
    pub provider: String,
    pub model: String,
    pub options: Value,
    pub style: String,
    pub cancellation_id: CancellationId,
    pub trigger: DependencyScheduleTrigger,
    pub expires_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScheduledRun {
    pub execution_id: String,
    pub schedule_id: String,
    pub terminal: bool,
    pub succeeded: bool,
    pub last_committed_sequence: Option<Sequence>,
    pub awaiting_continuation: Option<String>,
    pub error: Option<String>,
}

/// Dependency-owned request for one durable turn.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyRunTurnRequest {
    /// Existing session.
    pub session_id: SessionId,
    /// User-authored input.
    pub prompt: String,
    /// Explicit provider.
    pub provider: String,
    /// Explicit model.
    pub model: String,
    /// Provider options.
    pub options: Value,
    /// Optional caller-selected cancellation ID.
    pub cancellation_id: Option<CancellationId>,
}

/// Dependency-owned cancellation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCancelTurnRequest {
    /// Exact active operation.
    pub cancellation_id: CancellationId,
    /// Safe audit reason.
    pub reason: String,
}

/// Dependency-owned provider event.
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

/// Dependency-owned normalized turn result.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyRunTurnResponse {
    pub events: Vec<DependencyTurnEvent>,
    pub first_committed_sequence: Sequence,
    pub last_committed_sequence: Sequence,
    pub awaiting_continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyRunTurnStreamItem {
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

pub struct DependencyRunTurnStream {
    receiver: mpsc::Receiver<Result<DependencyRunTurnStreamItem, DependencyError>>,
}

impl DependencyRunTurnStream {
    #[must_use]
    pub fn next(&self) -> Option<Result<DependencyRunTurnStreamItem, DependencyError>> {
        self.receiver.recv().ok()
    }
}

/// Dependency-owned durable approval request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyResolveApprovalRequest {
    pub session_id: SessionId,
    pub continuation_id: String,
    pub approved: bool,
}

/// Dependency-owned durable approval response.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyResolveApprovalResponse {
    pub transitioned: bool,
    pub events: Vec<DependencyTurnEvent>,
    pub last_committed_sequence: Option<Sequence>,
    pub awaiting_continuation: Option<String>,
}

/// Dependency-owned runtime availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyRuntimeAvailability {
    /// The runtime reports that all required capabilities are ready.
    Ready,
    /// The runtime answered but one or more required capabilities are degraded.
    Degraded,
    /// The runtime endpoint could not provide usable health information.
    Unavailable,
}

/// Narrow outbound runtime interface consumed only by CLI data.
pub trait CliDependencyPort {
    /// Retrieves and normalizes runtime health through the runtime wire contract.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] when the request is invalid or the runtime
    /// response cannot be normalized.
    fn runtime_health(
        &self,
        request: DependencyRuntimeHealthRequest,
    ) -> Result<DependencyRuntimeHealthResponse, DependencyError>;

    /// Creates a durable runtime session.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport or unexpected-response failures.
    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreateSessionResponse, DependencyError>;

    /// Lists bounded dormant-session metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport or unexpected-response failures.
    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionSummary>, DependencyError>;

    /// Purely replays a session.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport or response normalization failures.
    fn inspect_session(
        &self,
        request: DependencyInspectSessionRequest,
    ) -> Result<DependencyInspectSessionResponse, DependencyError>;

    /// Reads one verified bounded reconnect page.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport, protocol, or response
    /// normalization failures.
    fn subscribe_session(
        &self,
        request: DependencySubscribeSessionRequest,
    ) -> Result<DependencySessionEventPage, DependencyError>;

    /// Atomically branches a session.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport or response normalization failures.
    fn branch_session(
        &self,
        request: DependencyBranchSessionRequest,
    ) -> Result<DependencyBranchSessionResponse, DependencyError>;

    /// Executes one durable runtime-owned turn.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport or response normalization failures.
    fn run_turn(
        &self,
        request: DependencyRunTurnRequest,
    ) -> Result<DependencyRunTurnResponse, DependencyError>;

    /// Starts a bounded incremental runtime turn.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] when the worker stream cannot be started.
    fn run_turn_stream(
        &self,
        request: DependencyRunTurnRequest,
    ) -> Result<DependencyRunTurnStream, DependencyError>;

    /// Cancels one active runtime turn.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport or unexpected-response failures.
    fn cancel_turn(&self, request: DependencyCancelTurnRequest) -> Result<(), DependencyError>;

    /// Resolves and, for the transition winner, resumes a durable tool approval.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for transport or response normalization failures.
    fn resolve_approval(
        &self,
        request: DependencyResolveApprovalRequest,
    ) -> Result<DependencyResolveApprovalResponse, DependencyError>;

    fn list_styles(&self) -> Result<Vec<DependencyStyleSummary>, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn inspect_style(
        &self,
        _request: DependencyInspectStyleRequest,
    ) -> Result<DependencyStyleInspection, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn validate_style(
        &self,
        _request: DependencyStyleFileRequest,
    ) -> Result<DependencyStyleValidationResult, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn compile_style(
        &self,
        _request: DependencyStyleFileRequest,
    ) -> Result<DependencyStyleInspection, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn upsert_schedule(
        &self,
        _schedule: DependencySchedule,
    ) -> Result<DependencyScheduleStoreResult, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn create_deferred_turn(
        &self,
        _request: DependencyCreateDeferredTurnRequest,
    ) -> Result<(), DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn remove_schedule(&self, _schedule_id: &str) -> Result<bool, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn list_schedules(&self, _limit: u32) -> Result<Vec<DependencySchedule>, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn claim_due_schedules(
        &self,
        _limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn complete_scheduled_execution(
        &self,
        _execution_id: &str,
        _succeeded: bool,
    ) -> Result<bool, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn run_due_schedules(
        &self,
        _limit: u32,
    ) -> Result<Vec<DependencyScheduledRun>, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }
}

/// Deterministic local runtime client used by the current local composition root and tests.
///
/// The client still constructs and decodes the versioned runtime protocol boundary. A
/// named-pipe or Unix-socket transport can replace it without changing CLI data or logic.
#[derive(Clone, Debug, PartialEq)]
pub struct DeterministicRuntimeClient {
    wire_response: RuntimeResponse,
}

impl DeterministicRuntimeClient {
    /// Creates a client that deterministically reports a ready runtime.
    #[must_use]
    pub fn ready(version: impl Into<String>) -> Self {
        Self {
            wire_response: RuntimeResponse::Health {
                status: "ok".into(),
                version: version.into(),
            },
        }
    }

    /// Creates a client with a deterministic wire response for dependency tests.
    #[cfg(test)]
    fn with_wire_response(wire_response: RuntimeResponse) -> Self {
        Self { wire_response }
    }

    fn send(&self, request: &RuntimeRequest) -> Result<RuntimeResponse, DependencyError> {
        if *request != RuntimeRequest::Health {
            return Err(DependencyError::UnsupportedRuntimeRequest);
        }
        Ok(self.wire_response.clone())
    }
}

impl CliDependencyPort for DeterministicRuntimeClient {
    fn runtime_health(
        &self,
        request: DependencyRuntimeHealthRequest,
    ) -> Result<DependencyRuntimeHealthResponse, DependencyError> {
        if request.client_label.trim().is_empty() {
            return Err(DependencyError::EmptyClientLabel);
        }

        let response = self.send(&RuntimeRequest::Health)?;
        let RuntimeResponse::Health { status, version } = response else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        let availability = match status.as_str() {
            "ok" => DependencyRuntimeAvailability::Ready,
            "degraded" => DependencyRuntimeAvailability::Degraded,
            "unavailable" => DependencyRuntimeAvailability::Unavailable,
            _ => return Err(DependencyError::UnknownRuntimeStatus(status)),
        };
        Ok(DependencyRuntimeHealthResponse {
            availability,
            runtime_version: version,
        })
    }

    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreateSessionResponse, DependencyError> {
        let response = self.send(&RuntimeRequest::CreateSession {
            workspace: request.workspace,
            style: request.style,
        })?;
        let RuntimeResponse::SessionCreated { session_id } = response else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyCreateSessionResponse { session_id })
    }

    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionSummary>, DependencyError> {
        let response = self.send(&RuntimeRequest::ListSessions {
            limit: request.limit,
        })?;
        let RuntimeResponse::Sessions { sessions } = response else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(sessions
            .into_iter()
            .map(|session| DependencySessionSummary {
                id: session.id,
                workspace_label: session.workspace_label,
                style: session.style,
                sequence: session.sequence,
                state: session.state,
            })
            .collect())
    }

    fn inspect_session(
        &self,
        _request: DependencyInspectSessionRequest,
    ) -> Result<DependencyInspectSessionResponse, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn subscribe_session(
        &self,
        _request: DependencySubscribeSessionRequest,
    ) -> Result<DependencySessionEventPage, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn branch_session(
        &self,
        _request: DependencyBranchSessionRequest,
    ) -> Result<DependencyBranchSessionResponse, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn run_turn(
        &self,
        _request: DependencyRunTurnRequest,
    ) -> Result<DependencyRunTurnResponse, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn run_turn_stream(
        &self,
        _request: DependencyRunTurnRequest,
    ) -> Result<DependencyRunTurnStream, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn cancel_turn(&self, _request: DependencyCancelTurnRequest) -> Result<(), DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }

    fn resolve_approval(
        &self,
        _request: DependencyResolveApprovalRequest,
    ) -> Result<DependencyResolveApprovalResponse, DependencyError> {
        Err(DependencyError::UnsupportedRuntimeRequest)
    }
}

/// Authenticated local runtime socket/named-pipe client.
#[derive(Clone, Debug)]
pub struct LocalRuntimeClient {
    endpoint: String,
    authorization_token: Arc<str>,
    maximum_frame_bytes: usize,
}

impl LocalRuntimeClient {
    /// Creates a fail-closed local runtime client.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for an empty endpoint, short secret, or
    /// unsupported frame bound.
    pub fn new(
        endpoint: String,
        authorization_token: String,
        maximum_frame_bytes: usize,
    ) -> Result<Self, DependencyError> {
        if endpoint.trim().is_empty() {
            return Err(DependencyError::InvalidConfiguration("empty endpoint"));
        }
        if authorization_token.len() < 32 {
            return Err(DependencyError::InvalidConfiguration(
                "authorization token is too short",
            ));
        }
        if maximum_frame_bytes == 0 || maximum_frame_bytes > DEFAULT_MAX_FRAME_BYTES {
            return Err(DependencyError::InvalidConfiguration(
                "frame bound is unsupported",
            ));
        }
        Ok(Self {
            endpoint,
            authorization_token: authorization_token.into(),
            maximum_frame_bytes,
        })
    }

    fn send_local(&self, request: RuntimeRequest) -> Result<RuntimeResponse, DependencyError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| DependencyError::Transport)?;
        runtime.block_on(self.send_async(request))
    }

    #[cfg(unix)]
    async fn send_async(
        &self,
        request: RuntimeRequest,
    ) -> Result<RuntimeResponse, DependencyError> {
        let mut stream = tokio::net::UnixStream::connect(&self.endpoint)
            .await
            .map_err(|_| DependencyError::Transport)?;
        self.exchange(&mut stream, request).await
    }

    #[cfg(windows)]
    async fn send_async(
        &self,
        request: RuntimeRequest,
    ) -> Result<RuntimeResponse, DependencyError> {
        let mut stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&self.endpoint)
            .map_err(|_| DependencyError::Transport)?;
        self.exchange(&mut stream, request).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one transport exchange validates unary, turn-stream, and subscription envelopes"
    )]
    async fn exchange<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
    ) -> Result<RuntimeResponse, DependencyError>
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
                        String::from("cancellation"),
                        String::from("bounded_backpressure"),
                        String::from("credit_windows"),
                        String::from("idempotency"),
                        String::from("request_response"),
                        String::from("streaming"),
                    ]),
                    authorization_token: self.authorization_token.to_string(),
                },
            },
            self.maximum_frame_bytes,
        )
        .await
        .map_err(|_| DependencyError::Transport)?;
        let negotiated: WireFrame<Negotiated> = read_frame(stream, self.maximum_frame_bytes)
            .await
            .map_err(|_| DependencyError::Transport)?;
        validate_response_header(&negotiated.header, &handshake_header)?;
        if !negotiated
            .payload
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        {
            return Err(DependencyError::Protocol);
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
        .map_err(|_| DependencyError::Transport)?;
        let response: WireFrame<RuntimeResponse> = read_frame(stream, self.maximum_frame_bytes)
            .await
            .map_err(|_| DependencyError::Transport)?;
        if response.header.kind == FrameKind::Response {
            validate_response_header(&response.header, &request_header)?;
            return Ok(response.payload);
        }
        let mut next_sequence = 1_u64;
        let mut turn_events = Vec::new();
        let mut session_events = Vec::new();
        let mut current = response;
        loop {
            validate_stream_header(&current.header, &request_header, next_sequence)?;
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
                        _ => return Err(DependencyError::UnexpectedRuntimeResponse),
                    }
                    if credit_windows {
                        write_window_update(
                            stream,
                            &request_header,
                            next_sequence,
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
                        _ => Err(DependencyError::UnexpectedRuntimeResponse),
                    };
                }
                _ => return Err(DependencyError::Protocol),
            }
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or(DependencyError::Protocol)?;
            current = read_frame(stream, self.maximum_frame_bytes)
                .await
                .map_err(|_| DependencyError::Transport)?;
        }
    }

    #[cfg(unix)]
    async fn send_stream_async(
        &self,
        request: RuntimeRequest,
        sender: mpsc::SyncSender<Result<DependencyRunTurnStreamItem, DependencyError>>,
    ) -> Result<(), DependencyError> {
        let mut stream = tokio::net::UnixStream::connect(&self.endpoint)
            .await
            .map_err(|_| DependencyError::Transport)?;
        self.exchange_stream(&mut stream, request, sender).await
    }

    #[cfg(windows)]
    async fn send_stream_async(
        &self,
        request: RuntimeRequest,
        sender: mpsc::SyncSender<Result<DependencyRunTurnStreamItem, DependencyError>>,
    ) -> Result<(), DependencyError> {
        let mut stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&self.endpoint)
            .map_err(|_| DependencyError::Transport)?;
        self.exchange_stream(&mut stream, request, sender).await
    }

    async fn exchange_stream<S>(
        &self,
        stream: &mut S,
        request: RuntimeRequest,
        sender: mpsc::SyncSender<Result<DependencyRunTurnStreamItem, DependencyError>>,
    ) -> Result<(), DependencyError>
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
                        String::from("credit_windows"),
                        String::from("streaming"),
                    ]),
                    authorization_token: self.authorization_token.to_string(),
                },
            },
            self.maximum_frame_bytes,
        )
        .await
        .map_err(|_| DependencyError::Transport)?;
        let negotiated: WireFrame<Negotiated> = read_frame(stream, self.maximum_frame_bytes)
            .await
            .map_err(|_| DependencyError::Transport)?;
        validate_response_header(&negotiated.header, &handshake_header)?;
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
        .map_err(|_| DependencyError::Transport)?;
        let mut expected_sequence = 1_u64;
        loop {
            let frame: WireFrame<RuntimeResponse> = read_frame(stream, self.maximum_frame_bytes)
                .await
                .map_err(|_| DependencyError::Transport)?;
            validate_stream_header(&frame.header, &request_header, expected_sequence)?;
            match frame.payload {
                RuntimeResponse::TurnEvent {
                    event,
                    committed_sequence,
                } if frame.header.kind == FrameKind::StreamItem => {
                    sender
                        .send(Ok(DependencyRunTurnStreamItem::Event {
                            event: map_turn_event(event),
                            committed_sequence,
                        }))
                        .map_err(|_| DependencyError::Transport)?;
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
                        .send(Ok(DependencyRunTurnStreamItem::Complete {
                            first_committed_sequence,
                            last_committed_sequence,
                            awaiting_continuation,
                        }))
                        .map_err(|_| DependencyError::Transport)?;
                    return Ok(());
                }
                _ => return Err(DependencyError::UnexpectedRuntimeResponse),
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(DependencyError::Protocol)?;
        }
    }
}

impl CliDependencyPort for LocalRuntimeClient {
    fn runtime_health(
        &self,
        request: DependencyRuntimeHealthRequest,
    ) -> Result<DependencyRuntimeHealthResponse, DependencyError> {
        if request.client_label.trim().is_empty() {
            return Err(DependencyError::EmptyClientLabel);
        }
        let RuntimeResponse::Health { status, version } =
            self.send_local(RuntimeRequest::Health)?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        let availability = match status.as_str() {
            "ok" => DependencyRuntimeAvailability::Ready,
            "degraded" => DependencyRuntimeAvailability::Degraded,
            "unavailable" => DependencyRuntimeAvailability::Unavailable,
            _ => return Err(DependencyError::UnknownRuntimeStatus(status)),
        };
        Ok(DependencyRuntimeHealthResponse {
            availability,
            runtime_version: version,
        })
    }

    fn list_styles(&self) -> Result<Vec<DependencyStyleSummary>, DependencyError> {
        let RuntimeResponse::Styles { styles } = self.send_local(RuntimeRequest::ListStyles)?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(styles.into_iter().map(map_style_summary).collect())
    }

    fn inspect_style(
        &self,
        request: DependencyInspectStyleRequest,
    ) -> Result<DependencyStyleInspection, DependencyError> {
        let RuntimeResponse::StyleInspected { inspection } =
            self.send_local(RuntimeRequest::InspectStyle {
                selector: request.selector,
            })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(map_style_inspection(inspection))
    }

    fn validate_style(
        &self,
        request: DependencyStyleFileRequest,
    ) -> Result<DependencyStyleValidationResult, DependencyError> {
        let (manifest, format) = read_style_manifest(&request.file)?;
        let RuntimeResponse::StyleValidated { valid, diagnostics } =
            self.send_local(RuntimeRequest::ValidateStyle { manifest, format })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyStyleValidationResult {
            valid,
            diagnostics: diagnostics.into_iter().map(map_style_diagnostic).collect(),
        })
    }

    fn compile_style(
        &self,
        request: DependencyStyleFileRequest,
    ) -> Result<DependencyStyleInspection, DependencyError> {
        let (manifest, format) = read_style_manifest(&request.file)?;
        let RuntimeResponse::StyleCompiled { inspection } =
            self.send_local(RuntimeRequest::CompileStyle { manifest, format })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(map_style_inspection(inspection))
    }

    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreateSessionResponse, DependencyError> {
        let RuntimeResponse::SessionCreated { session_id } =
            self.send_local(RuntimeRequest::CreateSession {
                workspace: request.workspace,
                style: request.style,
            })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyCreateSessionResponse { session_id })
    }

    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionSummary>, DependencyError> {
        let RuntimeResponse::Sessions { sessions } =
            self.send_local(RuntimeRequest::ListSessions {
                limit: request.limit,
            })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(sessions
            .into_iter()
            .map(|session| DependencySessionSummary {
                id: session.id,
                workspace_label: session.workspace_label,
                style: session.style,
                sequence: session.sequence,
                state: session.state,
            })
            .collect())
    }

    fn inspect_session(
        &self,
        request: DependencyInspectSessionRequest,
    ) -> Result<DependencyInspectSessionResponse, DependencyError> {
        let wire_request = if request.replay {
            RuntimeRequest::ReplaySession {
                session_id: request.session_id,
                at: request.at,
            }
        } else {
            RuntimeRequest::InspectSession {
                session_id: request.session_id,
                at: request.at,
            }
        };
        let RuntimeResponse::SessionInspected {
            session_id,
            head_sequence,
            inspected_sequence,
            event_count,
            state,
        } = self.send_local(wire_request)?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyInspectSessionResponse {
            session_id,
            head_sequence,
            inspected_sequence,
            event_count,
            state,
        })
    }

    fn subscribe_session(
        &self,
        request: DependencySubscribeSessionRequest,
    ) -> Result<DependencySessionEventPage, DependencyError> {
        let RuntimeResponse::SessionEvents {
            events,
            head_sequence,
            last_delivered_sequence,
            has_more,
        } = self.send_local(RuntimeRequest::Subscribe {
            session_id: request.session_id,
            after: request.after,
            limit: request.limit,
        })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
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

    fn branch_session(
        &self,
        request: DependencyBranchSessionRequest,
    ) -> Result<DependencyBranchSessionResponse, DependencyError> {
        let RuntimeResponse::SessionBranched {
            session_id,
            parent_session_id,
            fork_sequence,
            child_head_sequence,
        } = self.send_local(RuntimeRequest::BranchSession {
            session_id: request.session_id,
            at: request.at,
            style: request.style,
        })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyBranchSessionResponse {
            session_id,
            parent_session_id,
            fork_sequence,
            child_head_sequence,
        })
    }

    fn run_turn(
        &self,
        request: DependencyRunTurnRequest,
    ) -> Result<DependencyRunTurnResponse, DependencyError> {
        let RuntimeResponse::Turn {
            events,
            first_committed_sequence,
            last_committed_sequence,
            awaiting_continuation,
        } = self.send_local(RuntimeRequest::RunTurn {
            session_id: request.session_id,
            prompt: request.prompt,
            provider: request.provider,
            model: request.model,
            options: request.options,
            cancellation_id: request
                .cancellation_id
                .unwrap_or_else(|| CancellationId::from_uuid(Uuid::now_v7())),
        })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyRunTurnResponse {
            events: events.into_iter().map(map_turn_event).collect(),
            first_committed_sequence,
            last_committed_sequence,
            awaiting_continuation,
        })
    }

    fn run_turn_stream(
        &self,
        request: DependencyRunTurnRequest,
    ) -> Result<DependencyRunTurnStream, DependencyError> {
        let cancellation_id = request
            .cancellation_id
            .unwrap_or_else(|| CancellationId::from_uuid(Uuid::now_v7()));
        let wire = RuntimeRequest::RunTurn {
            session_id: request.session_id,
            prompt: request.prompt,
            provider: request.provider,
            model: request.model,
            options: request.options,
            cancellation_id,
        };
        let client = self.clone();
        let (sender, receiver) = mpsc::sync_channel(16);
        let error_sender = sender.clone();
        std::thread::Builder::new()
            .name(String::from("agentmod-cli-runtime-stream"))
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| DependencyError::Transport)
                    .and_then(|runtime| runtime.block_on(client.send_stream_async(wire, sender)));
                if let Err(error) = result {
                    let _ = error_sender.send(Err(error));
                }
            })
            .map_err(|_| DependencyError::Transport)?;
        Ok(DependencyRunTurnStream { receiver })
    }

    fn cancel_turn(&self, request: DependencyCancelTurnRequest) -> Result<(), DependencyError> {
        let RuntimeResponse::Cancelled = self.send_local(RuntimeRequest::Cancel {
            cancellation_id: request.cancellation_id,
            reason: request.reason,
        })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(())
    }

    fn resolve_approval(
        &self,
        request: DependencyResolveApprovalRequest,
    ) -> Result<DependencyResolveApprovalResponse, DependencyError> {
        let RuntimeResponse::ApprovalResolved {
            transitioned,
            events,
            last_committed_sequence,
            awaiting_continuation,
        } = self.send_local(RuntimeRequest::ResolveApproval {
            session_id: request.session_id,
            continuation_id: request.continuation_id,
            approved: request.approved,
            resume_after_resolution: true,
        })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyResolveApprovalResponse {
            transitioned,
            events: events.into_iter().map(map_turn_event).collect(),
            last_committed_sequence,
            awaiting_continuation,
        })
    }

    fn upsert_schedule(
        &self,
        schedule: DependencySchedule,
    ) -> Result<DependencyScheduleStoreResult, DependencyError> {
        let RuntimeResponse::ScheduleStored {
            schedule_id,
            replayed,
        } = self.send_local(RuntimeRequest::UpsertSchedule {
            schedule: Box::new(to_wire_schedule(schedule)),
        })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(DependencyScheduleStoreResult {
            schedule_id,
            replayed,
        })
    }

    fn create_deferred_turn(
        &self,
        request: DependencyCreateDeferredTurnRequest,
    ) -> Result<(), DependencyError> {
        let continuation_id = request.continuation_id;
        let RuntimeResponse::DeferredTurnCreated {
            continuation_id: created,
        } = self.send_local(RuntimeRequest::CreateDeferredTurn {
            session_id: request.session_id,
            continuation_id: continuation_id.clone(),
            schedule_id: request.schedule_id,
            prompt: request.prompt,
            workspace: request.workspace,
            provider: request.provider,
            model: request.model,
            options: request.options,
            style: request.style,
            cancellation_id: request.cancellation_id,
            trigger: to_wire_trigger(request.trigger),
            expires_at_ms: request.expires_at_ms,
        })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        if created != continuation_id {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        }
        Ok(())
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, DependencyError> {
        let RuntimeResponse::ScheduleRemoved { existed } =
            self.send_local(RuntimeRequest::RemoveSchedule {
                schedule_id: schedule_id.to_owned(),
            })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(existed)
    }

    fn list_schedules(&self, limit: u32) -> Result<Vec<DependencySchedule>, DependencyError> {
        let RuntimeResponse::Schedules { schedules } =
            self.send_local(RuntimeRequest::ListSchedules { limit })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(schedules.into_iter().map(from_wire_schedule).collect())
    }

    fn claim_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, DependencyError> {
        let RuntimeResponse::ScheduledExecutions { executions } =
            self.send_local(RuntimeRequest::ClaimDueSchedules { limit })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(executions
            .into_iter()
            .map(|execution| DependencyScheduledExecution {
                execution_id: execution.execution_id,
                scheduled_for_ms: execution.scheduled_for_ms,
                claimed_at_ms: execution.claimed_at_ms,
                schedule: from_wire_schedule(execution.schedule),
            })
            .collect())
    }

    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, DependencyError> {
        let RuntimeResponse::ScheduledExecutionCompleted { changed } =
            self.send_local(RuntimeRequest::CompleteScheduledExecution {
                execution_id: execution_id.to_owned(),
                succeeded,
            })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(changed)
    }

    fn run_due_schedules(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledRun>, DependencyError> {
        let RuntimeResponse::ScheduledRuns { runs } =
            self.send_local(RuntimeRequest::RunDueSchedules { limit })?
        else {
            return Err(DependencyError::UnexpectedRuntimeResponse);
        };
        Ok(runs
            .into_iter()
            .map(|run| DependencyScheduledRun {
                execution_id: run.execution_id,
                schedule_id: run.schedule_id,
                terminal: run.terminal,
                succeeded: run.succeeded,
                last_committed_sequence: run.last_committed_sequence,
                awaiting_continuation: run.awaiting_continuation,
                error: run.error,
            })
            .collect())
    }
}

fn to_wire_schedule(value: DependencySchedule) -> RuntimeScheduleSpec {
    RuntimeScheduleSpec {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: to_wire_trigger(value.trigger),
        payload: match value.payload {
            DependencySchedulePayload::Prompt { prompt } => {
                RuntimeSchedulePayload::Prompt { prompt }
            }
            DependencySchedulePayload::Continuation { continuation_id } => {
                RuntimeSchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn to_wire_trigger(value: DependencyScheduleTrigger) -> RuntimeScheduleTrigger {
    match value {
        DependencyScheduleTrigger::AtMillis(value) => RuntimeScheduleTrigger::AtMillis(value),
        DependencyScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        } => RuntimeScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        },
        DependencyScheduleTrigger::RuntimeEvent { event_type } => {
            RuntimeScheduleTrigger::RuntimeEvent { event_type }
        }
        DependencyScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => RuntimeScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        },
    }
}

fn from_wire_schedule(value: RuntimeScheduleSpec) -> DependencySchedule {
    DependencySchedule {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            RuntimeScheduleTrigger::AtMillis(value) => DependencyScheduleTrigger::AtMillis(value),
            RuntimeScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => DependencyScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            RuntimeScheduleTrigger::RuntimeEvent { event_type } => {
                DependencyScheduleTrigger::RuntimeEvent { event_type }
            }
            RuntimeScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => DependencyScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            RuntimeSchedulePayload::Prompt { prompt } => {
                DependencySchedulePayload::Prompt { prompt }
            }
            RuntimeSchedulePayload::Continuation { continuation_id } => {
                DependencySchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
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

fn read_style_manifest(
    file: &str,
) -> Result<(String, RuntimeStyleManifestFormat), DependencyError> {
    let path = Path::new(file);
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    let format = match extension.as_deref() {
        Some("toml") => RuntimeStyleManifestFormat::Toml,
        Some("json") => RuntimeStyleManifestFormat::Json,
        _ => return Err(DependencyError::UnsupportedStyleManifestExtension),
    };
    let metadata = std::fs::metadata(path).map_err(|_| DependencyError::StyleManifestFile)?;
    if metadata.len() > MAX_STYLE_MANIFEST_BYTES {
        return Err(DependencyError::StyleManifestTooLarge);
    }
    let manifest = std::fs::read_to_string(path).map_err(|_| DependencyError::StyleManifestFile)?;
    Ok((manifest, format))
}

fn map_style_summary(summary: RuntimeStyleSummary) -> DependencyStyleSummary {
    DependencyStyleSummary {
        id: summary.id,
        version: summary.version,
        source: match summary.source {
            RuntimeStyleSourceKind::BuiltIn => DependencyStyleSourceKind::BuiltIn,
            RuntimeStyleSourceKind::User => DependencyStyleSourceKind::User,
            RuntimeStyleSourceKind::Project => DependencyStyleSourceKind::Project,
            RuntimeStyleSourceKind::Plugin => DependencyStyleSourceKind::Plugin,
            RuntimeStyleSourceKind::Inline => DependencyStyleSourceKind::Inline,
        },
        availability: match summary.availability {
            RuntimeStyleAvailability::Available => DependencyStyleAvailability::Available,
            RuntimeStyleAvailability::Disabled => DependencyStyleAvailability::Disabled,
            RuntimeStyleAvailability::Invalid => DependencyStyleAvailability::Invalid,
            RuntimeStyleAvailability::Incompatible => DependencyStyleAvailability::Incompatible,
            RuntimeStyleAvailability::Conflict => DependencyStyleAvailability::Conflict,
        },
        style_content_hash: summary.style_content_hash,
        compiled_cache_key: summary.compiled_cache_key,
        required_capabilities: summary.required_capabilities,
    }
}

fn map_style_diagnostic(diagnostic: RuntimeStyleDiagnostic) -> DependencyStyleDiagnostic {
    DependencyStyleDiagnostic {
        code: diagnostic.code,
        path: diagnostic.path,
        message: diagnostic.message,
        help: diagnostic.help,
    }
}

fn map_style_inspection(inspection: RuntimeStyleInspection) -> DependencyStyleInspection {
    DependencyStyleInspection {
        summary: map_style_summary(inspection.summary),
        source_locator: inspection.source_locator,
        manifest: inspection.manifest,
        compiled: inspection.compiled,
        diagnostics: inspection
            .diagnostics
            .into_iter()
            .map(map_style_diagnostic)
            .collect(),
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
) -> Result<(), DependencyError>
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
    .map_err(|_| DependencyError::Transport)
}

fn validate_response_header(
    response: &FrameHeader,
    request: &FrameHeader,
) -> Result<(), DependencyError> {
    if response.family != "runtime"
        || response.kind != FrameKind::Response
        || !response
            .version
            .is_compatible_with(RUNTIME_PROTOCOL_VERSION)
        || response.request_id != request.request_id
        || response.correlation_id != request.correlation_id
        || response.idempotency_id != request.idempotency_id
    {
        return Err(DependencyError::Protocol);
    }
    Ok(())
}

fn validate_stream_header(
    response: &FrameHeader,
    request: &FrameHeader,
    expected_sequence: u64,
) -> Result<(), DependencyError> {
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
        return Err(DependencyError::Protocol);
    }
    Ok(())
}

/// CLI dependency-layer failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DependencyError {
    /// The data layer supplied no diagnostic identity for the client.
    #[error("runtime client label is empty")]
    EmptyClientLabel,
    /// The local adapter received a request it cannot represent.
    #[error("runtime client does not support the requested operation")]
    UnsupportedRuntimeRequest,
    /// The runtime returned a response for a different operation.
    #[error("runtime returned an unexpected response")]
    UnexpectedRuntimeResponse,
    /// The runtime returned a health state not supported by this client version.
    #[error("runtime returned unknown health status `{0}`")]
    UnknownRuntimeStatus(String),
    /// Bootstrap configuration is unsafe.
    #[error("runtime client configuration is invalid: {0}")]
    InvalidConfiguration(&'static str),
    /// The manifest path could not be read as UTF-8 text.
    #[error("style manifest file could not be read")]
    StyleManifestFile,
    /// Only TOML and JSON manifest files are accepted by the CLI boundary.
    #[error("style manifest file must use a .toml or .json extension")]
    UnsupportedStyleManifestExtension,
    /// Manifest size exceeds the bounded local input limit.
    #[error("style manifest file exceeds the 1 MiB limit")]
    StyleManifestTooLarge,
    /// Local transport was unavailable or malformed.
    #[error("runtime transport is unavailable")]
    Transport,
    /// Runtime framing/negotiation metadata was inconsistent.
    #[error("runtime protocol validation failed")]
    Protocol,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> DependencyRuntimeHealthRequest {
        DependencyRuntimeHealthRequest {
            client_label: "agentmod-cli".into(),
        }
    }

    #[test]
    fn ready_wire_health_is_normalized() {
        assert_eq!(
            DeterministicRuntimeClient::ready("0.1.0")
                .runtime_health(request())
                .expect("runtime health"),
            DependencyRuntimeHealthResponse {
                availability: DependencyRuntimeAvailability::Ready,
                runtime_version: "0.1.0".into(),
            }
        );
    }

    #[test]
    fn degraded_wire_health_is_normalized() {
        let client = DeterministicRuntimeClient::with_wire_response(RuntimeResponse::Health {
            status: "degraded".into(),
            version: "0.1.0".into(),
        });
        assert_eq!(
            client
                .runtime_health(request())
                .expect("runtime health")
                .availability,
            DependencyRuntimeAvailability::Degraded
        );
    }

    #[test]
    fn unexpected_wire_response_is_rejected() {
        let client = DeterministicRuntimeClient::with_wire_response(RuntimeResponse::Cancelled);
        assert_eq!(
            client.runtime_health(request()),
            Err(DependencyError::UnexpectedRuntimeResponse)
        );
    }

    #[test]
    fn unknown_wire_status_is_rejected() {
        let client = DeterministicRuntimeClient::with_wire_response(RuntimeResponse::Health {
            status: "mystery".into(),
            version: "0.1.0".into(),
        });
        assert_eq!(
            client.runtime_health(request()),
            Err(DependencyError::UnknownRuntimeStatus("mystery".into()))
        );
    }

    #[tokio::test]
    async fn local_exchange_authenticates_negotiates_and_validates_response_identity() {
        let client = LocalRuntimeClient::new(
            String::from("fixture"),
            String::from("0123456789abcdef0123456789abcdef"),
            4096,
        )
        .expect("client");
        let (mut caller, mut server) = tokio::io::duplex(16 * 1024);
        let server_task = tokio::spawn(async move {
            let handshake: WireFrame<Handshake> =
                read_frame(&mut server, 4096).await.expect("handshake");
            assert_eq!(
                handshake.payload.authorization_token,
                "0123456789abcdef0123456789abcdef"
            );
            write_frame(
                &mut server,
                &WireFrame {
                    header: response_for(&handshake.header),
                    payload: Negotiated {
                        version: RUNTIME_PROTOCOL_VERSION,
                        capabilities: BTreeSet::from([String::from("request_response")]),
                    },
                },
                4096,
            )
            .await
            .expect("negotiated");
            let request: WireFrame<RuntimeRequest> =
                read_frame(&mut server, 4096).await.expect("request");
            assert_eq!(request.payload, RuntimeRequest::Health);
            write_frame(
                &mut server,
                &WireFrame {
                    header: response_for(&request.header),
                    payload: RuntimeResponse::Health {
                        status: String::from("ok"),
                        version: String::from("test"),
                    },
                },
                4096,
            )
            .await
            .expect("response");
        });
        assert_eq!(
            client
                .exchange(&mut caller, RuntimeRequest::Health)
                .await
                .expect("exchange"),
            RuntimeResponse::Health {
                status: String::from("ok"),
                version: String::from("test")
            }
        );
        server_task.await.expect("server");
    }

    #[tokio::test]
    async fn local_exchange_round_trips_style_list_fixture() {
        let client = LocalRuntimeClient::new(
            String::from("fixture"),
            String::from("0123456789abcdef0123456789abcdef"),
            4096,
        )
        .expect("client");
        let (mut caller, mut server) = tokio::io::duplex(16 * 1024);
        let server_task = tokio::spawn(async move {
            let handshake: WireFrame<Handshake> =
                read_frame(&mut server, 4096).await.expect("handshake");
            write_frame(
                &mut server,
                &WireFrame {
                    header: response_for(&handshake.header),
                    payload: Negotiated {
                        version: RUNTIME_PROTOCOL_VERSION,
                        capabilities: BTreeSet::from([String::from("request_response")]),
                    },
                },
                4096,
            )
            .await
            .expect("negotiated");
            let request: WireFrame<RuntimeRequest> =
                read_frame(&mut server, 4096).await.expect("request");
            assert_eq!(request.payload, RuntimeRequest::ListStyles);
            write_frame(
                &mut server,
                &WireFrame {
                    header: response_for(&request.header),
                    payload: RuntimeResponse::Styles {
                        styles: vec![RuntimeStyleSummary {
                            id: String::from("calm"),
                            version: String::from("1.0.0"),
                            source: RuntimeStyleSourceKind::BuiltIn,
                            availability: RuntimeStyleAvailability::Available,
                            style_content_hash: String::from("hash"),
                            compiled_cache_key: String::from("cache"),
                            required_capabilities: vec![],
                        }],
                    },
                },
                4096,
            )
            .await
            .expect("response");
        });
        let response = client
            .exchange(&mut caller, RuntimeRequest::ListStyles)
            .await
            .expect("exchange");
        assert!(matches!(response, RuntimeResponse::Styles { styles } if styles[0].id == "calm"));
        server_task.await.expect("server");
    }

    #[tokio::test]
    async fn local_exchange_collects_ordered_incremental_turn_frames() {
        let client = LocalRuntimeClient::new(
            String::from("fixture"),
            String::from("0123456789abcdef0123456789abcdef"),
            4096,
        )
        .expect("client");
        let (mut caller, mut server) = tokio::io::duplex(16 * 1024);
        let server_task = tokio::spawn(async move {
            let handshake: WireFrame<Handshake> =
                read_frame(&mut server, 4096).await.expect("handshake");
            write_frame(
                &mut server,
                &WireFrame {
                    header: response_for(&handshake.header),
                    payload: Negotiated {
                        version: RUNTIME_PROTOCOL_VERSION,
                        capabilities: BTreeSet::from([String::from("streaming")]),
                    },
                },
                4096,
            )
            .await
            .expect("negotiated");
            let request: WireFrame<RuntimeRequest> =
                read_frame(&mut server, 4096).await.expect("request");
            for (sequence, payload) in [
                (
                    1,
                    RuntimeResponse::TurnEvent {
                        event: RuntimeProviderEvent::Started,
                        committed_sequence: Sequence::new(4).expect("sequence"),
                    },
                ),
                (
                    2,
                    RuntimeResponse::TurnEvent {
                        event: RuntimeProviderEvent::Text {
                            text: String::from("incremental"),
                        },
                        committed_sequence: Sequence::new(5).expect("sequence"),
                    },
                ),
            ] {
                write_frame(
                    &mut server,
                    &WireFrame {
                        header: stream_for(&request.header, FrameKind::StreamItem, sequence),
                        payload,
                    },
                    4096,
                )
                .await
                .expect("stream item");
            }
            write_frame(
                &mut server,
                &WireFrame {
                    header: stream_for(&request.header, FrameKind::StreamEnd, 3),
                    payload: RuntimeResponse::TurnComplete {
                        first_committed_sequence: Sequence::new(2).expect("sequence"),
                        last_committed_sequence: Sequence::new(6).expect("sequence"),
                        awaiting_continuation: None,
                    },
                },
                4096,
            )
            .await
            .expect("stream end");
        });
        let response = client
            .exchange(
                &mut caller,
                RuntimeRequest::RunTurn {
                    session_id: SessionId::from_uuid(Uuid::from_u128(1)),
                    prompt: String::from("hello"),
                    provider: String::from("deterministic-mock"),
                    model: String::from("mock-model"),
                    options: serde_json::json!({}),
                    cancellation_id: CancellationId::from_uuid(Uuid::from_u128(2)),
                },
            )
            .await
            .expect("stream");
        assert!(matches!(
            response,
            RuntimeResponse::Turn {
                events,
                first_committed_sequence,
                last_committed_sequence,
                awaiting_continuation: None,
            } if events == vec![
                RuntimeProviderEvent::Started,
                RuntimeProviderEvent::Text { text: String::from("incremental") },
            ] && first_committed_sequence.get() == 2 && last_committed_sequence.get() == 6
        ));
        server_task.await.expect("server");
    }

    fn response_for(request: &FrameHeader) -> FrameHeader {
        FrameHeader {
            family: String::from("runtime"),
            version: RUNTIME_PROTOCOL_VERSION,
            kind: FrameKind::Response,
            request_id: request.request_id,
            stream_sequence: request.stream_sequence,
            correlation_id: request.correlation_id,
            causation_id: request.causation_id,
            idempotency_id: request.idempotency_id,
            cancellation_id: request.cancellation_id,
        }
    }

    fn stream_for(request: &FrameHeader, kind: FrameKind, sequence: u64) -> FrameHeader {
        FrameHeader {
            kind,
            stream_sequence: Some(sequence),
            ..response_for(request)
        }
    }
}
