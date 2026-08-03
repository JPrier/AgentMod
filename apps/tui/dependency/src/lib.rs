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
    fs::File,
    io::Read as _,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
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
    RuntimeSchedulePayload, RuntimeScheduleSpec, RuntimeScheduleTrigger, RuntimeSessionEvent,
    RuntimeStyleAvailability, RuntimeStyleSourceKind,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

const RUNTIME_PROTOCOL_VERSION: Version = Version::new(2, 5);
const MAX_ATTACHMENT_BYTES: u64 = 512 * 1024;
const MAX_ATTACHMENT_NAME_BYTES: usize = 255;

#[cfg(windows)]
const PIPE_OPEN_ATTEMPTS: usize = 20;
#[cfg(windows)]
const PIPE_OPEN_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Dependency-owned attachment representation after confined file loading.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyAttachmentKind {
    Image,
    Audio,
    Blob,
}

/// Dependency-owned bounded attachment with no operating-system handles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAttachment {
    pub identity: String,
    pub name: String,
    pub uri: String,
    pub mime_type: String,
    pub kind: DependencyAttachmentKind,
    pub data_base64: String,
    pub byte_size: u64,
}

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

/// Dependency-owned style diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleDiagnostic {
    pub code: String,
    pub path: String,
    pub message: String,
    pub help: String,
}

/// Dependency-owned complete style inspection.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyStyleInspection {
    pub summary: DependencyStyleSummary,
    pub source_locator: String,
    pub manifest: Value,
    pub compiled: Option<Value>,
    pub diagnostics: Vec<DependencyStyleDiagnostic>,
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

/// Dependency-owned plugin lifecycle action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginLifecycleAction {
    Disable,
    Enable,
    Quarantine,
    Unquarantine,
}

/// Dependency-owned exact plugin lifecycle request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginLifecycleRequest {
    pub session_id: SessionId,
    pub plugin_id: String,
    pub action: DependencyPluginLifecycleAction,
    pub reason_code: Option<String>,
    pub cancellation_id: CancellationId,
}

/// Dependency-owned canonical plugin lifecycle result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginLifecycleResponse {
    pub session_id: SessionId,
    pub plugin_id: String,
    pub plugin_version: String,
    pub state: String,
    pub committed_sequence: Sequence,
    pub replayed: bool,
}

/// Dependency-owned MCP OAuth management action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyMcpOAuthAction {
    Begin,
    Status,
    Cancel { transaction_id: String },
}

/// Dependency-owned exact MCP OAuth management request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyMcpOAuthRequest {
    pub session_id: SessionId,
    pub server_id: String,
    pub action: DependencyMcpOAuthAction,
    pub cancellation_id: CancellationId,
}

/// Dependency-owned bounded MCP OAuth result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyMcpOAuthResponse {
    Started {
        server_id: String,
        transaction_id: String,
        authorization_url: String,
        authorization_url_hash: String,
        expires_at_ms: i64,
    },
    Status {
        server_id: String,
        status: String,
        transaction_id: Option<String>,
        expires_at_ms: Option<i64>,
        scopes: Vec<String>,
        status_hash: String,
    },
}

/// Dependency-owned replay-only artifact row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyArtifactResource {
    pub execution_id: String,
    pub node_id: String,
    pub state: String,
    pub mime_type: String,
    pub byte_size: u64,
    pub artifact_reference: Option<String>,
}

/// Dependency-owned replay-only child row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyChildResource {
    pub execution_id: String,
    pub task_id: String,
    pub state: String,
    pub child_style: String,
    pub workspace_mode: String,
    pub child_session_id: Option<String>,
    pub summary: Option<String>,
}

/// Dependency-owned replay-only process reconciliation row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProcessResource {
    pub call_id: String,
    pub process_id: String,
    pub status: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
}

/// Dependency-owned bounded canonical runtime-resource projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRuntimeResources {
    pub artifacts: Vec<DependencyArtifactResource>,
    pub children: Vec<DependencyChildResource>,
    pub processes: Vec<DependencyProcessResource>,
}

/// Dependency-owned schedule trigger.
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

/// Dependency-owned schedule payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencySchedulePayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
    GraphTrigger { run_id: String, node_id: String },
}

/// Dependency-owned durable schedule.
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

/// Dependency-owned schedule storage result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScheduleStoreResponse {
    pub schedule_id: String,
    pub replayed: bool,
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

/// Bounded dependency-owned stream of newly committed session events.
pub struct DependencySessionEventStream {
    receiver: mpsc::Receiver<Result<DependencySessionEvent, TuiDependencyError>>,
    cancelled: Arc<AtomicBool>,
}

impl DependencySessionEventStream {
    #[must_use]
    pub fn try_next(&self) -> Option<Result<DependencySessionEvent, TuiDependencyError>> {
        match self.receiver.try_recv() {
            Ok(value) => Some(value),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => Some(Err(TuiDependencyError::Transport)),
        }
    }
}

impl Drop for DependencySessionEventStream {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
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
    fn load_attachment(
        &self,
        _workspace: String,
        _path: String,
    ) -> Result<DependencyAttachment, TuiDependencyError> {
        Err(TuiDependencyError::AttachmentUnavailable)
    }
    fn health(&self) -> Result<DependencyRuntimeHealth, TuiDependencyError>;
    fn list_styles(&self) -> Result<Vec<DependencyStyleSummary>, TuiDependencyError>;
    fn inspect_style(
        &self,
        _selector: String,
    ) -> Result<DependencyStyleInspection, TuiDependencyError> {
        Err(TuiDependencyError::UnexpectedResponse)
    }
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
    fn inspect_runtime_resources(
        &self,
        _session_id: SessionId,
    ) -> Result<DependencyRuntimeResources, TuiDependencyError> {
        Ok(DependencyRuntimeResources {
            artifacts: Vec::new(),
            children: Vec::new(),
            processes: Vec::new(),
        })
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
    fn change_plugin_lifecycle(
        &self,
        _request: DependencyPluginLifecycleRequest,
    ) -> Result<DependencyPluginLifecycleResponse, TuiDependencyError> {
        Err(TuiDependencyError::UnexpectedResponse)
    }
    fn manage_mcp_oauth(
        &self,
        _request: DependencyMcpOAuthRequest,
    ) -> Result<DependencyMcpOAuthResponse, TuiDependencyError> {
        Err(TuiDependencyError::UnexpectedResponse)
    }
    fn upsert_schedule(
        &self,
        _schedule: DependencySchedule,
    ) -> Result<DependencyScheduleStoreResponse, TuiDependencyError> {
        Err(TuiDependencyError::UnexpectedResponse)
    }
    fn list_schedules(&self, _limit: u32) -> Result<Vec<DependencySchedule>, TuiDependencyError> {
        Err(TuiDependencyError::UnexpectedResponse)
    }
    fn remove_schedule(&self, _schedule_id: &str) -> Result<bool, TuiDependencyError> {
        Err(TuiDependencyError::UnexpectedResponse)
    }
    fn session_events(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
        limit: u32,
    ) -> Result<DependencySessionEventPage, TuiDependencyError>;
    fn start_session_subscription(
        &self,
        _session_id: SessionId,
        _after: Option<Sequence>,
    ) -> Result<DependencySessionEventStream, TuiDependencyError> {
        Err(TuiDependencyError::UnexpectedResponse)
    }
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

#[cfg(windows)]
async fn open_named_pipe_client(
    endpoint: &str,
) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    let mut attempt = 0_usize;
    loop {
        match tokio::net::windows::named_pipe::ClientOptions::new().open(endpoint) {
            Ok(client) => return Ok(client),
            Err(error) if attempt + 1 < PIPE_OPEN_ATTEMPTS && retryable_named_pipe_open(&error) => {
                attempt += 1;
                tokio::time::sleep(PIPE_OPEN_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(windows)]
fn retryable_named_pipe_open(error: &std::io::Error) -> bool {
    // Win32 ERROR_FILE_NOT_FOUND, ERROR_PIPE_BUSY, and
    // ERROR_PIPE_NOT_CONNECTED. Other failures are never retried.
    matches!(error.raw_os_error(), Some(2 | 231 | 233))
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
        let mut stream = open_named_pipe_client(&self.endpoint)
            .await
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
        let mut stream = open_named_pipe_client(&self.endpoint)
            .await
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
    fn load_attachment(
        &self,
        workspace: String,
        path: String,
    ) -> Result<DependencyAttachment, TuiDependencyError> {
        load_workspace_attachment(&workspace, &path)
    }
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

    fn inspect_style(
        &self,
        selector: String,
    ) -> Result<DependencyStyleInspection, TuiDependencyError> {
        let RuntimeResponse::StyleInspected { inspection } =
            self.send(RuntimeRequest::InspectStyle { selector })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(DependencyStyleInspection {
            summary: DependencyStyleSummary {
                id: inspection.summary.id,
                version: inspection.summary.version,
                source: match inspection.summary.source {
                    RuntimeStyleSourceKind::BuiltIn => DependencyStyleSourceKind::BuiltIn,
                    RuntimeStyleSourceKind::User => DependencyStyleSourceKind::User,
                    RuntimeStyleSourceKind::Project => DependencyStyleSourceKind::Project,
                    RuntimeStyleSourceKind::Plugin => DependencyStyleSourceKind::Plugin,
                    RuntimeStyleSourceKind::Inline => DependencyStyleSourceKind::Inline,
                },
                availability: match inspection.summary.availability {
                    RuntimeStyleAvailability::Available => DependencyStyleAvailability::Available,
                    RuntimeStyleAvailability::Disabled => DependencyStyleAvailability::Disabled,
                    RuntimeStyleAvailability::Invalid => DependencyStyleAvailability::Invalid,
                    RuntimeStyleAvailability::Incompatible => {
                        DependencyStyleAvailability::Incompatible
                    }
                    RuntimeStyleAvailability::Conflict => DependencyStyleAvailability::Conflict,
                },
                style_content_hash: inspection.summary.style_content_hash,
                compiled_cache_key: inspection.summary.compiled_cache_key,
                required_capabilities: inspection.summary.required_capabilities,
            },
            source_locator: inspection.source_locator,
            manifest: inspection.manifest,
            compiled: inspection.compiled,
            diagnostics: inspection
                .diagnostics
                .into_iter()
                .map(|diagnostic| DependencyStyleDiagnostic {
                    code: diagnostic.code,
                    path: diagnostic.path,
                    message: diagnostic.message,
                    help: diagnostic.help,
                })
                .collect(),
        })
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
        let RuntimeResponse::SessionInspected {
            session_id: inspected,
            state,
            ..
        } = self.send(RuntimeRequest::InspectSession {
            session_id,
            at: None,
        })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        if inspected != session_id {
            return Err(TuiDependencyError::UnexpectedResponse);
        }
        Ok(state)
    }

    fn inspect_runtime_resources(
        &self,
        session_id: SessionId,
    ) -> Result<DependencyRuntimeResources, TuiDependencyError> {
        let state = self.inspect_session(session_id)?;
        parse_runtime_resources(&state)
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

    fn change_plugin_lifecycle(
        &self,
        request: DependencyPluginLifecycleRequest,
    ) -> Result<DependencyPluginLifecycleResponse, TuiDependencyError> {
        let wire_request = match request.action {
            DependencyPluginLifecycleAction::Disable => RuntimeRequest::DisablePlugin {
                session_id: request.session_id,
                plugin_id: request.plugin_id.clone(),
                cancellation_id: request.cancellation_id,
            },
            DependencyPluginLifecycleAction::Enable => RuntimeRequest::EnablePlugin {
                session_id: request.session_id,
                plugin_id: request.plugin_id.clone(),
                cancellation_id: request.cancellation_id,
            },
            DependencyPluginLifecycleAction::Quarantine => RuntimeRequest::QuarantinePlugin {
                session_id: request.session_id,
                plugin_id: request.plugin_id.clone(),
                reason_code: request
                    .reason_code
                    .clone()
                    .ok_or(TuiDependencyError::InvalidPluginLifecycleRequest)?,
                cancellation_id: request.cancellation_id,
            },
            DependencyPluginLifecycleAction::Unquarantine => RuntimeRequest::UnquarantinePlugin {
                session_id: request.session_id,
                plugin_id: request.plugin_id.clone(),
                cancellation_id: request.cancellation_id,
            },
        };
        if request.action != DependencyPluginLifecycleAction::Quarantine
            && request.reason_code.is_some()
        {
            return Err(TuiDependencyError::InvalidPluginLifecycleRequest);
        }
        let RuntimeResponse::PluginLifecycleChanged {
            session_id,
            plugin_id,
            plugin_version,
            state,
            committed_sequence,
            replayed,
        } = self.send(wire_request)?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        if session_id != request.session_id || plugin_id != request.plugin_id {
            return Err(TuiDependencyError::UnexpectedResponse);
        }
        Ok(DependencyPluginLifecycleResponse {
            session_id,
            plugin_id,
            plugin_version,
            state,
            committed_sequence,
            replayed,
        })
    }

    fn manage_mcp_oauth(
        &self,
        request: DependencyMcpOAuthRequest,
    ) -> Result<DependencyMcpOAuthResponse, TuiDependencyError> {
        validate_mcp_component(&request.server_id)
            .map_err(|()| TuiDependencyError::InvalidMcpOAuthRequest)?;
        let wire_request = match &request.action {
            DependencyMcpOAuthAction::Begin => RuntimeRequest::McpOAuthBegin {
                session_id: request.session_id,
                server_id: request.server_id.clone(),
                cancellation_id: request.cancellation_id,
            },
            DependencyMcpOAuthAction::Status => RuntimeRequest::McpOAuthStatus {
                session_id: request.session_id,
                server_id: request.server_id.clone(),
                cancellation_id: request.cancellation_id,
            },
            DependencyMcpOAuthAction::Cancel { transaction_id } => {
                validate_mcp_transaction(transaction_id)
                    .map_err(|()| TuiDependencyError::InvalidMcpOAuthRequest)?;
                RuntimeRequest::McpOAuthCancel {
                    session_id: request.session_id,
                    server_id: request.server_id.clone(),
                    transaction_id: transaction_id.clone(),
                    cancellation_id: request.cancellation_id,
                }
            }
        };
        match (request.action, self.send(wire_request)?) {
            (
                DependencyMcpOAuthAction::Begin,
                RuntimeResponse::McpOAuthStarted {
                    server_id,
                    transaction_id,
                    authorization_url,
                    authorization_url_hash,
                    expires_at_ms,
                },
            ) => {
                if server_id != request.server_id
                    || validate_mcp_transaction(&transaction_id).is_err()
                    || validate_mcp_authorization_url(&authorization_url).is_err()
                    || !valid_hash(&authorization_url_hash)
                    || expires_at_ms <= 0
                {
                    return Err(TuiDependencyError::InvalidMcpOAuthOutcome);
                }
                Ok(DependencyMcpOAuthResponse::Started {
                    server_id,
                    transaction_id,
                    authorization_url,
                    authorization_url_hash,
                    expires_at_ms,
                })
            }
            (
                DependencyMcpOAuthAction::Status | DependencyMcpOAuthAction::Cancel { .. },
                RuntimeResponse::McpOAuthStatus {
                    server_id,
                    status,
                    transaction_id,
                    expires_at_ms,
                    scopes,
                    status_hash,
                },
            ) => {
                if server_id != request.server_id
                    || !matches!(
                        status.as_str(),
                        "unauthorized" | "pending" | "authorized" | "failed"
                    )
                    || transaction_id
                        .as_deref()
                        .is_some_and(|value| validate_mcp_transaction(value).is_err())
                    || expires_at_ms.is_some_and(|value| value <= 0)
                    || scopes.len() > 64
                    || scopes
                        .iter()
                        .any(|scope| scope.is_empty() || scope.len() > 256)
                    || !valid_hash(&status_hash)
                {
                    return Err(TuiDependencyError::InvalidMcpOAuthOutcome);
                }
                Ok(DependencyMcpOAuthResponse::Status {
                    server_id,
                    status,
                    transaction_id,
                    expires_at_ms,
                    scopes,
                    status_hash,
                })
            }
            _ => Err(TuiDependencyError::UnexpectedResponse),
        }
    }

    fn upsert_schedule(
        &self,
        schedule: DependencySchedule,
    ) -> Result<DependencyScheduleStoreResponse, TuiDependencyError> {
        let expected_schedule_id = schedule.schedule_id.clone();
        let RuntimeResponse::ScheduleStored {
            schedule_id,
            replayed,
        } = self.send(RuntimeRequest::UpsertSchedule {
            schedule: Box::new(to_wire_schedule(schedule)),
        })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        if schedule_id != expected_schedule_id {
            return Err(TuiDependencyError::UnexpectedResponse);
        }
        Ok(DependencyScheduleStoreResponse {
            schedule_id,
            replayed,
        })
    }

    fn list_schedules(&self, limit: u32) -> Result<Vec<DependencySchedule>, TuiDependencyError> {
        let RuntimeResponse::Schedules { schedules } =
            self.send(RuntimeRequest::ListSchedules { limit })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(schedules.into_iter().map(from_wire_schedule).collect())
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, TuiDependencyError> {
        let RuntimeResponse::ScheduleRemoved { existed } =
            self.send(RuntimeRequest::RemoveSchedule {
                schedule_id: schedule_id.to_owned(),
            })?
        else {
            return Err(TuiDependencyError::UnexpectedResponse);
        };
        Ok(existed)
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

    fn start_session_subscription(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
    ) -> Result<DependencySessionEventStream, TuiDependencyError> {
        let dependency = self.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let (sender, receiver) = mpsc::sync_channel(64);
        std::thread::Builder::new()
            .name(String::from("agentmod-tui-session-subscription"))
            .spawn(move || {
                let mut cursor = after;
                while !worker_cancelled.load(Ordering::Acquire) {
                    match dependency.session_events(session_id, cursor, 512) {
                        Ok(page) => {
                            for event in page.events {
                                cursor = Some(event.sequence);
                                if sender.send(Ok(event)).is_err() {
                                    return;
                                }
                            }
                            if page.has_more {
                                continue;
                            }
                        }
                        Err(error) => {
                            let _ = sender.send(Err(error));
                            return;
                        }
                    }
                    for _ in 0..20 {
                        if worker_cancelled.load(Ordering::Acquire) {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            })
            .map_err(|_| TuiDependencyError::Transport)?;
        Ok(DependencySessionEventStream {
            receiver,
            cancelled,
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

fn to_wire_schedule(schedule: DependencySchedule) -> RuntimeScheduleSpec {
    RuntimeScheduleSpec {
        schedule_id: schedule.schedule_id,
        session_id: schedule.session_id,
        idempotency_id: schedule.idempotency_id,
        style: schedule.style,
        workspace: schedule.workspace,
        permission_policy: schedule.permission_policy,
        provider: schedule.provider,
        model: schedule.model,
        token_budget: schedule.token_budget,
        cost_budget_micros: schedule.cost_budget_micros,
        trigger: match schedule.trigger {
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
        },
        payload: match schedule.payload {
            DependencySchedulePayload::Prompt { prompt } => {
                RuntimeSchedulePayload::Prompt { prompt }
            }
            DependencySchedulePayload::Continuation { continuation_id } => {
                RuntimeSchedulePayload::Continuation { continuation_id }
            }
            DependencySchedulePayload::GraphTrigger { run_id, node_id } => {
                RuntimeSchedulePayload::GraphTrigger { run_id, node_id }
            }
        },
        active: schedule.active,
    }
}

fn from_wire_schedule(schedule: RuntimeScheduleSpec) -> DependencySchedule {
    DependencySchedule {
        schedule_id: schedule.schedule_id,
        session_id: schedule.session_id,
        idempotency_id: schedule.idempotency_id,
        style: schedule.style,
        workspace: schedule.workspace,
        permission_policy: schedule.permission_policy,
        provider: schedule.provider,
        model: schedule.model,
        token_budget: schedule.token_budget,
        cost_budget_micros: schedule.cost_budget_micros,
        trigger: match schedule.trigger {
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
        payload: match schedule.payload {
            RuntimeSchedulePayload::Prompt { prompt } => {
                DependencySchedulePayload::Prompt { prompt }
            }
            RuntimeSchedulePayload::Continuation { continuation_id } => {
                DependencySchedulePayload::Continuation { continuation_id }
            }
            RuntimeSchedulePayload::GraphTrigger { run_id, node_id } => {
                DependencySchedulePayload::GraphTrigger { run_id, node_id }
            }
        },
        active: schedule.active,
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
            ..
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

fn validate_mcp_component(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(())
    } else {
        Ok(())
    }
}

fn validate_mcp_transaction(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(())
    } else {
        Ok(())
    }
}

fn validate_mcp_authorization_url(value: &str) -> Result<(), ()> {
    if value.len() > 8_192
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(());
    }
    if value
        .strip_prefix("https://")
        .is_some_and(|remainder| !remainder.is_empty())
    {
        return Ok(());
    }
    let Some(remainder) = value.strip_prefix("http://") else {
        return Err(());
    };
    let authority = remainder
        .split(['/', '?', '#'])
        .next()
        .filter(|authority| !authority.is_empty() && !authority.contains('@'))
        .ok_or(())?;
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        let (host, suffix) = bracketed.split_once(']').ok_or(())?;
        if !suffix.is_empty()
            && (!suffix.starts_with(':')
                || suffix[1..].is_empty()
                || !suffix[1..].bytes().all(|byte| byte.is_ascii_digit()))
        {
            return Err(());
        }
        host
    } else if let Some((host, port)) = authority.rsplit_once(':') {
        if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(());
        }
        host
    } else {
        authority
    };
    if matches!(host, "localhost" | "127.0.0.1" | "::1") {
        Ok(())
    } else {
        Err(())
    }
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn parse_runtime_resources(
    state: &Value,
) -> Result<DependencyRuntimeResources, TuiDependencyError> {
    const MAX_RESOURCES: usize = 512;
    let root = state
        .as_object()
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?;
    let artifacts = bounded_object(root.get("artifact_persistences"), MAX_RESOURCES)?
        .iter()
        .map(|(key, value)| parse_artifact_resource(key, value))
        .collect::<Result<Vec<_>, _>>()?;
    let children = bounded_object(root.get("child_agents"), MAX_RESOURCES)?
        .iter()
        .map(|(key, value)| parse_child_resource(key, value))
        .collect::<Result<Vec<_>, _>>()?;
    let processes = bounded_object(root.get("process_reconciliations"), MAX_RESOURCES)?
        .iter()
        .map(|(key, value)| parse_process_resource(key, value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DependencyRuntimeResources {
        artifacts,
        children,
        processes,
    })
}

fn bounded_object(
    value: Option<&Value>,
    maximum: usize,
) -> Result<&serde_json::Map<String, Value>, TuiDependencyError> {
    let object = value
        .and_then(Value::as_object)
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?;
    if object.len() > maximum {
        return Err(TuiDependencyError::InvalidRuntimeResourceProjection);
    }
    Ok(object)
}

fn parse_artifact_resource(
    key: &str,
    value: &Value,
) -> Result<DependencyArtifactResource, TuiDependencyError> {
    let object = value
        .as_object()
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?;
    let identity = object
        .get("identity")
        .and_then(Value::as_object)
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?;
    let execution_id = bounded_string(identity.get("execution_id"), 512)?;
    if execution_id != key {
        return Err(TuiDependencyError::InvalidRuntimeResourceProjection);
    }
    Ok(DependencyArtifactResource {
        execution_id,
        node_id: bounded_string(identity.get("node_id"), 256)?,
        state: bounded_string(object.get("state"), 32)?,
        mime_type: bounded_string(object.get("mime_type"), 256)?,
        byte_size: object
            .get("byte_size")
            .and_then(Value::as_u64)
            .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?,
        artifact_reference: optional_bounded_string(object.get("artifact_reference"), 1_024)?,
    })
}

fn parse_child_resource(
    key: &str,
    value: &Value,
) -> Result<DependencyChildResource, TuiDependencyError> {
    let object = value
        .as_object()
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?;
    let identity = object
        .get("identity")
        .and_then(Value::as_object)
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?;
    let execution_id = bounded_string(identity.get("execution_id"), 512)?;
    if execution_id != key {
        return Err(TuiDependencyError::InvalidRuntimeResourceProjection);
    }
    Ok(DependencyChildResource {
        execution_id,
        task_id: bounded_string(identity.get("task_id"), 256)?,
        state: bounded_string(object.get("state"), 32)?,
        child_style: bounded_string(object.get("child_style"), 256)?,
        workspace_mode: bounded_string(object.get("workspace_mode"), 64)?,
        child_session_id: optional_bounded_string(object.get("child_session_id"), 128)?,
        summary: optional_bounded_string(object.get("summary"), 8_192)?,
    })
}

fn parse_process_resource(
    key: &str,
    value: &Value,
) -> Result<DependencyProcessResource, TuiDependencyError> {
    let object = value
        .as_object()
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?;
    let call_id = bounded_string(object.get("call_id"), 512)?;
    if call_id != key {
        return Err(TuiDependencyError::InvalidRuntimeResourceProjection);
    }
    Ok(DependencyProcessResource {
        call_id,
        process_id: bounded_string(object.get("process_id"), 512)?,
        status: optional_bounded_string(object.get("status"), 64)?,
        started_at: object
            .get("started_at")
            .and_then(Value::as_u64)
            .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)?,
        completed_at: optional_u64(object.get("completed_at"))?,
    })
}

fn bounded_string(value: Option<&Value>, maximum: usize) -> Result<String, TuiDependencyError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= maximum)
        .map(str::to_owned)
        .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection)
}

fn optional_bounded_string(
    value: Option<&Value>,
    maximum: usize,
) -> Result<Option<String>, TuiDependencyError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => bounded_string(Some(value), maximum).map(Some),
    }
}

fn optional_u64(value: Option<&Value>) -> Result<Option<u64>, TuiDependencyError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or(TuiDependencyError::InvalidRuntimeResourceProjection),
    }
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

fn load_workspace_attachment(
    workspace: &str,
    requested: &str,
) -> Result<DependencyAttachment, TuiDependencyError> {
    if workspace.trim().is_empty() || requested.trim().is_empty() || requested.contains('\0') {
        return Err(TuiDependencyError::InvalidAttachmentPath);
    }
    let workspace_input = Path::new(workspace);
    let workspace = workspace_input
        .canonicalize()
        .map_err(|_| TuiDependencyError::InvalidAttachmentPath)?;
    if !workspace.is_dir() {
        return Err(TuiDependencyError::InvalidAttachmentPath);
    }
    let requested = Path::new(requested);
    if requested
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(TuiDependencyError::AttachmentOutsideWorkspace);
    }
    let relative = if requested.is_absolute() {
        requested
            .strip_prefix(workspace_input)
            .or_else(|_| requested.strip_prefix(&workspace))
            .map_err(|_| TuiDependencyError::AttachmentOutsideWorkspace)?
            .to_path_buf()
    } else {
        requested.to_path_buf()
    };
    validate_attachment_relative_path(&relative)?;
    let (mut file, opened_path) = open_confined_attachment(&workspace, &relative)?;
    let (bytes, byte_size) = read_bounded_attachment(&mut file)?;
    let name = opened_path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty() && value.len() <= MAX_ATTACHMENT_NAME_BYTES)
        .ok_or(TuiDependencyError::InvalidAttachmentPath)?;
    if secret_like_name(name) {
        return Err(TuiDependencyError::SecretAttachment);
    }
    if secret_like_content(&bytes) {
        return Err(TuiDependencyError::SecretAttachment);
    }
    let (kind, mime_type) = attachment_type(&opened_path, &bytes)?;
    Ok(DependencyAttachment {
        identity: attachment_identity(&opened_path),
        name: name.to_owned(),
        uri: file_uri(&opened_path),
        mime_type: mime_type.to_owned(),
        kind,
        data_base64: BASE64.encode(bytes),
        byte_size,
    })
}

fn validate_attachment_relative_path(relative: &Path) -> Result<(), TuiDependencyError> {
    if relative.as_os_str().is_empty() {
        return Err(TuiDependencyError::InvalidAttachmentPath);
    }
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(TuiDependencyError::InvalidAttachmentPath);
        }
    }
    Ok(())
}

fn open_confined_attachment(
    root: &Path,
    relative: &Path,
) -> Result<(File, PathBuf), TuiDependencyError> {
    use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
    use cap_std::fs::OpenOptions as CapOpenOptions;

    let mut directory = open_workspace_capability(root)?;
    let components = relative.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            return Err(TuiDependencyError::InvalidAttachmentPath);
        };
        let last = index + 1 == components.len();
        if last {
            let mut options = CapOpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            let opened = directory.open_with(name, &options).map_err(|_error| {
                directory.symlink_metadata(name).map_or(
                    TuiDependencyError::InvalidAttachmentPath,
                    |metadata| {
                        if metadata.file_type().is_symlink() {
                            TuiDependencyError::AttachmentSymlink
                        } else if !metadata.file_type().is_file() {
                            TuiDependencyError::AttachmentNotFile
                        } else {
                            TuiDependencyError::InvalidAttachmentPath
                        }
                    },
                )
            })?;
            return Ok((opened.into_std(), root.join(relative)));
        }
        directory = directory.open_dir_nofollow(name).map_err(|_error| {
            if directory
                .symlink_metadata(name)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                TuiDependencyError::AttachmentSymlink
            } else {
                TuiDependencyError::InvalidAttachmentPath
            }
        })?;
    }
    Err(TuiDependencyError::InvalidAttachmentPath)
}

fn open_workspace_capability(root: &Path) -> Result<cap_std::fs::Dir, TuiDependencyError> {
    use cap_fs_ext::DirExt as _;
    use cap_std::{ambient_authority, fs::Dir};

    let mut anchor = PathBuf::new();
    let mut descendants = Vec::new();
    for component in root.components() {
        match component {
            Component::Prefix(prefix) => anchor.push(prefix.as_os_str()),
            Component::RootDir => anchor.push(component.as_os_str()),
            Component::Normal(name) => descendants.push(name.to_owned()),
            _ => return Err(TuiDependencyError::InvalidAttachmentPath),
        }
    }
    if !anchor.has_root() {
        return Err(TuiDependencyError::InvalidAttachmentPath);
    }
    let mut directory = Dir::open_ambient_dir(anchor, ambient_authority())
        .map_err(|_| TuiDependencyError::InvalidAttachmentPath)?;
    for name in descendants {
        directory = directory
            .open_dir_nofollow(name)
            .map_err(|_| TuiDependencyError::AttachmentSymlink)?;
    }
    Ok(directory)
}

fn read_bounded_attachment(file: &mut File) -> Result<(Vec<u8>, u64), TuiDependencyError> {
    let before = file
        .metadata()
        .map_err(|_| TuiDependencyError::AttachmentRead)?;
    if !before.file_type().is_file() {
        return Err(TuiDependencyError::AttachmentNotFile);
    }
    if before.len() == 0 || before.len() > MAX_ATTACHMENT_BYTES {
        return Err(TuiDependencyError::AttachmentTooLarge);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(before.len()).unwrap_or(0));
    file.take(MAX_ATTACHMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| TuiDependencyError::AttachmentRead)?;
    let after = file
        .metadata()
        .map_err(|_| TuiDependencyError::AttachmentRead)?;
    let actual = u64::try_from(bytes.len()).map_err(|_| TuiDependencyError::AttachmentTooLarge)?;
    if actual == 0 || actual > MAX_ATTACHMENT_BYTES {
        return Err(TuiDependencyError::AttachmentTooLarge);
    }
    if !after.file_type().is_file() || before.len() != actual || after.len() != actual {
        return Err(TuiDependencyError::AttachmentChanged);
    }
    Ok((bytes, actual))
}

fn attachment_type(
    path: &Path,
    bytes: &[u8],
) -> Result<(DependencyAttachmentKind, &'static str), TuiDependencyError> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .ok_or(TuiDependencyError::UnsupportedAttachmentMime)?;
    match extension.as_str() {
        "png" if bytes.starts_with(b"\x89PNG\r\n\x1a\n") => {
            Ok((DependencyAttachmentKind::Image, "image/png"))
        }
        "jpg" | "jpeg" if bytes.starts_with(&[0xff, 0xd8, 0xff]) => {
            Ok((DependencyAttachmentKind::Image, "image/jpeg"))
        }
        "gif" if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") => {
            Ok((DependencyAttachmentKind::Image, "image/gif"))
        }
        "webp"
            if bytes.starts_with(b"RIFF") && bytes.get(8..12).is_some_and(|tag| tag == b"WEBP") =>
        {
            Ok((DependencyAttachmentKind::Image, "image/webp"))
        }
        "wav"
            if bytes.starts_with(b"RIFF") && bytes.get(8..12).is_some_and(|tag| tag == b"WAVE") =>
        {
            Ok((DependencyAttachmentKind::Audio, "audio/wav"))
        }
        "mp3"
            if bytes.starts_with(b"ID3")
                || bytes
                    .get(..2)
                    .is_some_and(|header| header[0] == 0xff && header[1] & 0xe0 == 0xe0) =>
        {
            Ok((DependencyAttachmentKind::Audio, "audio/mpeg"))
        }
        "ogg" if bytes.starts_with(b"OggS") => Ok((DependencyAttachmentKind::Audio, "audio/ogg")),
        "bin" => Ok((DependencyAttachmentKind::Blob, "application/octet-stream")),
        _ => Err(TuiDependencyError::UnsupportedAttachmentMime),
    }
}

fn secret_like_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == ".env"
        || lower.starts_with(".env.")
        || [".pem", ".key", ".p12", ".pfx"]
            .iter()
            .any(|suffix| lower.ends_with(suffix))
        || ["secret", "password", "credential", "private_key"]
            .iter()
            .any(|marker| lower.contains(marker))
}

fn secret_like_content(bytes: &[u8]) -> bool {
    let lower = bytes.iter().map(u8::to_ascii_lowercase).collect::<Vec<_>>();
    [
        b"-----begin private key-----".as_slice(),
        b"-----begin rsa private key-----".as_slice(),
        b"aws_secret_access_key".as_slice(),
        b"client_secret=".as_slice(),
        b"password=".as_slice(),
    ]
    .iter()
    .any(|marker| lower.windows(marker.len()).any(|window| window == *marker))
}

fn attachment_identity(path: &Path) -> String {
    let identity = normalized_attachment_path(path);
    if cfg!(windows) {
        identity.to_ascii_lowercase()
    } else {
        identity
    }
}

fn file_uri(path: &Path) -> String {
    let path = normalized_attachment_path(path);
    let path = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b':' | b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(&mut encoded, "%{byte:02X}").expect("writing to a string cannot fail");
        }
    }
    format!("file://{encoded}")
}

fn normalized_attachment_path(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    path.strip_prefix("//?/UNC/").map_or_else(
        || path.strip_prefix("//?/").unwrap_or(&path).to_owned(),
        |network| format!("//{network}"),
    )
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TuiDependencyError {
    #[error("TUI runtime dependency configuration is invalid")]
    InvalidConfiguration,
    #[error("TUI runtime transport is unavailable")]
    Transport,
    #[error("TUI runtime protocol validation failed")]
    Protocol,
    #[error("TUI plugin lifecycle request is invalid")]
    InvalidPluginLifecycleRequest,
    #[error("TUI MCP OAuth request is invalid")]
    InvalidMcpOAuthRequest,
    #[error("runtime returned an invalid MCP OAuth outcome")]
    InvalidMcpOAuthOutcome,
    #[error("runtime returned an invalid canonical resource projection")]
    InvalidRuntimeResourceProjection,
    #[error("TUI attachment loading is unavailable")]
    AttachmentUnavailable,
    #[error("attachment path is invalid")]
    InvalidAttachmentPath,
    #[error("attachment must remain inside the selected session workspace")]
    AttachmentOutsideWorkspace,
    #[error("symbolic-link attachments are prohibited")]
    AttachmentSymlink,
    #[error("attachment must be a regular file")]
    AttachmentNotFile,
    #[error("attachment must contain between 1 and 524288 bytes")]
    AttachmentTooLarge,
    #[error("secret-like attachments are prohibited")]
    SecretAttachment,
    #[error("attachment MIME type is unsupported or does not match its content")]
    UnsupportedAttachmentMime,
    #[error("attachment could not be read")]
    AttachmentRead,
    #[error("attachment changed while it was being read")]
    AttachmentChanged,
    #[error("runtime returned an unexpected TUI response")]
    UnexpectedResponse,
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::Write as _;

    use super::*;

    fn attachment_workspace() -> PathBuf {
        let root = std::env::temp_dir().join(format!("agentmod-tui-attachment-{}", Uuid::now_v7()));
        fs::create_dir(&root).expect("attachment workspace");
        root
    }

    #[test]
    fn workspace_loader_returns_bounded_typed_image_and_blob_records() {
        let root = attachment_workspace();
        let image = root.join("pixel.png");
        let blob = root.join("evidence.bin");
        fs::write(&image, b"\x89PNG\r\n\x1a\nfixture").expect("image fixture");
        fs::write(&blob, b"bounded blob evidence").expect("blob fixture");

        let loaded_image =
            load_workspace_attachment(root.to_str().expect("workspace path"), "pixel.png")
                .expect("load image");
        assert_eq!(loaded_image.kind, DependencyAttachmentKind::Image);
        assert_eq!(loaded_image.mime_type, "image/png");
        assert_eq!(loaded_image.data_base64, "iVBORw0KGgpmaXh0dXJl");
        assert!(loaded_image.uri.starts_with("file://"));
        assert!(!loaded_image.uri.contains("%3F"));

        let loaded_blob = load_workspace_attachment(
            root.to_str().expect("workspace path"),
            blob.to_str().expect("blob path"),
        )
        .expect("load blob");
        assert_eq!(loaded_blob.kind, DependencyAttachmentKind::Blob);
        assert_eq!(loaded_blob.mime_type, "application/octet-stream");
        assert_eq!(loaded_blob.byte_size, 21);
        fs::remove_dir_all(root).expect("remove attachment workspace");
    }

    #[test]
    fn workspace_loader_signature_validates_supported_audio() {
        let root = attachment_workspace();
        fs::write(root.join("sample.wav"), b"RIFF\x04\0\0\0WAVEdata").expect("wav");
        fs::write(root.join("sample.mp3"), b"ID3\x04\0\0fixture").expect("mp3");
        fs::write(root.join("sample.ogg"), b"OggS\0fixture").expect("ogg");
        fs::write(root.join("fake.wav"), b"not-wave").expect("fake wav");
        for (name, mime) in [
            ("sample.wav", "audio/wav"),
            ("sample.mp3", "audio/mpeg"),
            ("sample.ogg", "audio/ogg"),
        ] {
            let loaded = load_workspace_attachment(root.to_str().expect("workspace path"), name)
                .expect("load audio");
            assert_eq!(loaded.kind, DependencyAttachmentKind::Audio);
            assert_eq!(loaded.mime_type, mime);
        }
        assert_eq!(
            load_workspace_attachment(root.to_str().expect("workspace path"), "fake.wav"),
            Err(TuiDependencyError::UnsupportedAttachmentMime)
        );
        fs::remove_dir_all(root).expect("remove attachment workspace");
    }

    #[test]
    fn workspace_loader_rejects_escape_non_file_secret_mime_and_size() {
        let root = attachment_workspace();
        let outside = root.with_extension("bin");
        fs::write(&outside, b"outside").expect("outside fixture");
        fs::create_dir(root.join("directory.bin")).expect("directory fixture");
        fs::write(root.join("credentials.bin"), b"not disclosed").expect("secret name");
        fs::write(root.join("notes.txt"), b"unsupported").expect("unsupported fixture");
        fs::write(
            root.join("large.bin"),
            vec![0_u8; usize::try_from(MAX_ATTACHMENT_BYTES).expect("test bound fits usize") + 1],
        )
        .expect("large fixture");
        assert_eq!(
            load_workspace_attachment(
                root.to_str().expect("workspace path"),
                "../agentmod-tui-attachment-escape.bin"
            ),
            Err(TuiDependencyError::AttachmentOutsideWorkspace)
        );
        assert_eq!(
            load_workspace_attachment(root.to_str().expect("workspace path"), "directory.bin"),
            Err(TuiDependencyError::AttachmentNotFile)
        );
        assert_eq!(
            load_workspace_attachment(root.to_str().expect("workspace path"), "credentials.bin"),
            Err(TuiDependencyError::SecretAttachment)
        );
        assert_eq!(
            load_workspace_attachment(root.to_str().expect("workspace path"), "notes.txt"),
            Err(TuiDependencyError::UnsupportedAttachmentMime)
        );
        assert_eq!(
            load_workspace_attachment(root.to_str().expect("workspace path"), "large.bin"),
            Err(TuiDependencyError::AttachmentTooLarge)
        );
        fs::remove_file(outside).expect("remove outside fixture");
        fs::remove_dir_all(root).expect("remove attachment workspace");
    }

    #[test]
    fn opened_handle_is_stable_across_replacement_and_growth_is_bounded() {
        let root = attachment_workspace();
        let path = root.join("stable.bin");
        fs::write(&path, b"original bytes").expect("original fixture");
        let (mut opened, _) = open_confined_attachment(&root, Path::new("stable.bin"))
            .expect("open confined fixture");
        let moved = root.join("moved.bin");
        fs::rename(&path, &moved).expect("replace open path");
        fs::write(&path, b"replacement bytes").expect("replacement fixture");
        let (bytes, size) = read_bounded_attachment(&mut opened).expect("read stable handle");
        assert_eq!(bytes, b"original bytes");
        assert_eq!(size, 14);

        let growing = root.join("growing.bin");
        fs::write(&growing, b"small").expect("growing fixture");
        let (mut opened, _) = open_confined_attachment(&root, Path::new("growing.bin"))
            .expect("open growing fixture");
        OpenOptions::new()
            .append(true)
            .open(&growing)
            .expect("open append handle")
            .write_all(&vec![
                0_u8;
                usize::try_from(MAX_ATTACHMENT_BYTES)
                    .expect("attachment bound fits usize")
            ])
            .expect("grow fixture");
        assert_eq!(
            read_bounded_attachment(&mut opened),
            Err(TuiDependencyError::AttachmentTooLarge)
        );
        fs::remove_dir_all(root).expect("remove attachment workspace");
    }

    #[cfg(unix)]
    #[test]
    fn workspace_loader_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let root = attachment_workspace();
        fs::write(root.join("actual.bin"), b"actual").expect("target fixture");
        symlink(root.join("actual.bin"), root.join("linked.bin")).expect("symlink fixture");
        assert_eq!(
            load_workspace_attachment(root.to_str().expect("workspace path"), "linked.bin"),
            Err(TuiDependencyError::AttachmentSymlink)
        );
        fs::remove_dir_all(root).expect("remove attachment workspace");
    }

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

    #[cfg(windows)]
    #[test]
    fn named_pipe_open_retries_only_transient_instance_gap_errors() {
        for code in [2, 231, 233] {
            assert!(retryable_named_pipe_open(
                &std::io::Error::from_raw_os_error(code)
            ));
        }
        for code in [5, 87, 109] {
            assert!(!retryable_named_pipe_open(
                &std::io::Error::from_raw_os_error(code)
            ));
        }
    }

    #[test]
    fn every_schedule_trigger_and_payload_round_trips_the_dependency_boundary() {
        let triggers = vec![
            DependencyScheduleTrigger::AtMillis(123),
            DependencyScheduleTrigger::Interval {
                starts_at_ms: 456,
                every_ms: 789,
            },
            DependencyScheduleTrigger::RuntimeEvent {
                event_type: String::from("model.response_completed"),
            },
            DependencyScheduleTrigger::ProcessOutput {
                process_id: String::from("process-1"),
                contains: String::from("ready"),
            },
        ];
        let payloads = vec![
            DependencySchedulePayload::Prompt {
                prompt: String::from("run checks"),
            },
            DependencySchedulePayload::Continuation {
                continuation_id: String::from("continuation-1"),
            },
            DependencySchedulePayload::GraphTrigger {
                run_id: String::from("run-1"),
                node_id: String::from("delay"),
            },
        ];
        for trigger in triggers {
            for payload in &payloads {
                let schedule = DependencySchedule {
                    schedule_id: String::from("schedule-1"),
                    session_id: SessionId::from_uuid(Uuid::nil()),
                    idempotency_id: String::from("idempotency-1"),
                    style: String::from("persistent-chat"),
                    workspace: String::from("workspace"),
                    permission_policy: String::from("interactive"),
                    provider: String::from("deterministic-mock"),
                    model: String::from("mock-model"),
                    token_budget: 100_000,
                    cost_budget_micros: 0,
                    trigger: trigger.clone(),
                    payload: payload.clone(),
                    active: true,
                };
                assert_eq!(
                    from_wire_schedule(to_wire_schedule(schedule.clone())),
                    schedule
                );
            }
        }
    }

    #[test]
    fn mcp_oauth_inputs_and_transient_urls_are_strictly_bounded() {
        assert!(validate_mcp_component("server_1").is_ok());
        assert!(validate_mcp_component("server/1").is_err());
        assert!(validate_mcp_transaction("transaction-1").is_ok());
        assert!(validate_mcp_transaction("transaction:1").is_err());
        assert!(validate_mcp_authorization_url("https://id.example.test/authorize").is_ok());
        assert!(validate_mcp_authorization_url("http://localhost:8080/authorize").is_ok());
        assert!(validate_mcp_authorization_url("http://127.0.0.1/callback").is_ok());
        assert!(validate_mcp_authorization_url("http://[::1]:8080/callback").is_ok());
        assert!(validate_mcp_authorization_url("http://example.test/authorize").is_err());
        assert!(validate_mcp_authorization_url("javascript:alert(1)").is_err());
        assert!(valid_hash(&"a".repeat(64)));
        assert!(!valid_hash(&"z".repeat(64)));
    }

    #[test]
    fn canonical_runtime_resources_are_typed_bounded_and_identity_checked() {
        let state = serde_json::json!({
            "artifact_persistences": {
                "artifact-execution": {
                    "identity": {"execution_id": "artifact-execution", "node_id": "persist"},
                    "state": "completed",
                    "mime_type": "text/markdown",
                    "byte_size": 42,
                    "artifact_reference": "artifact:blake3:fixture"
                }
            },
            "child_agents": {
                "child-execution": {
                    "identity": {"execution_id": "child-execution", "task_id": "task-1"},
                    "state": "completed",
                    "child_style": "ephemeral-turn@1.2.0",
                    "workspace_mode": "shared_read_only",
                    "child_session_id": "00000000-0000-0000-0000-000000000001",
                    "summary": "done"
                }
            },
            "process_reconciliations": {
                "call-1": {
                    "call_id": "call-1",
                    "process_id": "process-1",
                    "started_at": 7,
                    "status": "live",
                    "completed_at": 8
                }
            }
        });
        let resources = parse_runtime_resources(&state).expect("canonical resources");
        assert_eq!(resources.artifacts[0].node_id, "persist");
        assert_eq!(resources.children[0].workspace_mode, "shared_read_only");
        assert_eq!(resources.processes[0].status.as_deref(), Some("live"));

        let mut substituted = state;
        substituted["child_agents"]["child-execution"]["identity"]["execution_id"] =
            Value::String(String::from("substituted"));
        assert!(matches!(
            parse_runtime_resources(&substituted),
            Err(TuiDependencyError::InvalidRuntimeResourceProjection)
        ));
    }
}
