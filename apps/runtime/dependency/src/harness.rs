//! Supervised local harness-process adapter.
#![allow(
    missing_docs,
    reason = "dependency-local transport records are self-describing"
)]
use agentmod_harness_protocol as wire;
use async_trait::async_trait;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyEntry {
    System(String),
    User(String),
    Assistant(String),
    ToolCall {
        call_id: String,
        tool: String,
        arguments: Value,
    },
    ToolResult {
        call_id: String,
        content: String,
        truncated: bool,
    },
    Summary {
        text: String,
        start: u64,
        end: u64,
    },
    Metadata {
        key: String,
        value: Value,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyDecision {
    Continue,
    Replace(Vec<DependencyEntry>),
    Reject(String),
    Cancel(String),
}
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyCommand {
    Execute {
        harness_id: String,
        session_id: String,
        provider: String,
        model: String,
        entries: Vec<DependencyEntry>,
        options: Value,
        grant: String,
        cancellation_id: String,
    },
    Continue {
        harness_id: String,
        continuation_id: String,
        decision: DependencyDecision,
    },
    Cancel {
        harness_id: String,
        cancellation_id: String,
    },
    Health {
        harness_id: String,
    },
}

impl DependencyCommand {
    /// Returns the adapter identity selected by runtime logic.
    #[must_use]
    pub fn harness_id(&self) -> &str {
        match self {
            Self::Execute { harness_id, .. }
            | Self::Continue { harness_id, .. }
            | Self::Cancel { harness_id, .. }
            | Self::Health { harness_id } => harness_id,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub reasoning_tokens: u64,
    pub estimated: bool,
    pub cost: Option<CostMetadata>,
}

/// Provider-neutral cost metadata carried with a completed exchange.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CostMetadata {
    /// Stable pricing-record source.
    pub source: String,
    /// Pricing-record version.
    pub version: String,
    /// Computed input cost in micro-units of `currency`.
    pub input_cost_micros: u64,
    /// Computed output cost in micro-units of `currency`.
    pub output_cost_micros: u64,
    /// Computed cache-read cost in micro-units of `currency`.
    pub cache_read_cost_micros: u64,
    /// Computed cache-write cost in micro-units of `currency`.
    pub cache_write_cost_micros: u64,
    /// ISO-4217 currency code; empty when the pricing record is unknown.
    pub currency: String,
}
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyEvent {
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
        usage: DependencyUsage,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyReply {
    Health {
        status: String,
        ready: u32,
        capabilities: Vec<String>,
    },
    Events(Vec<DependencyEvent>),
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

pub struct DependencyEventStream {
    receiver: mpsc::Receiver<Result<DependencyEvent, HarnessDependencyError>>,
}

impl DependencyEventStream {
    pub async fn next(&mut self) -> Option<Result<DependencyEvent, HarnessDependencyError>> {
        self.receiver.recv().await
    }

    pub(crate) const fn from_receiver(
        receiver: mpsc::Receiver<Result<DependencyEvent, HarnessDependencyError>>,
    ) -> Self {
        Self { receiver }
    }
}
#[derive(Clone, Debug)]
pub struct HarnessDependencyConfig {
    pub program: String,
    pub arguments: Vec<String>,
    pub maximum_frame_bytes: usize,
    pub request_timeout: Duration,
    pub frame_pacing: Duration,
    pub authorization_key: [u8; 32],
    pub maximum_connections: usize,
    pub maximum_pending_connections: usize,
    pub test_gate_root: Option<PathBuf>,
}
struct Connection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}
#[derive(Clone)]
pub struct ProcessHarnessDependency {
    config: Arc<HarnessDependencyConfig>,
    connections: Arc<Mutex<Vec<Connection>>>,
    connection_capacity: Arc<Semaphore>,
    pending_connections: Arc<AtomicUsize>,
    cancellations: Arc<Mutex<CancellationRegistry>>,
}

#[derive(Default)]
struct CancellationRegistry {
    active: BTreeMap<String, CancellationToken>,
    pending: BTreeSet<String>,
}

const MAX_PENDING_CANCELLATIONS: usize = 1_024;
#[async_trait]
pub trait HarnessDependencyPort: Send + Sync {
    async fn exchange(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyReply, HarnessDependencyError>;
    async fn exchange_events(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyEventStream, HarnessDependencyError>;
    async fn shutdown(&self);
}
impl ProcessHarnessDependency {
    /// Generates one runtime-lifetime harness authorization key.
    #[must_use]
    pub fn generate_authorization_key() -> [u8; 32] {
        let first = uuid::Uuid::now_v7();
        let second = uuid::Uuid::now_v7();
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(first.as_bytes());
        key[16..].copy_from_slice(second.as_bytes());
        key
    }

    /// Constructs a lazy supervised harness adapter.
    ///
    /// # Errors
    ///
    /// Rejects missing programs and invalid transport bounds.
    pub fn new(config: HarnessDependencyConfig) -> Result<Self, HarnessDependencyError> {
        if config.program.trim().is_empty()
            || config.program.contains('\0')
            || config.arguments.iter().any(|v| v.contains('\0'))
            || config.maximum_frame_bytes == 0
            || config.request_timeout.is_zero()
            || config.frame_pacing > Duration::from_secs(5)
            || config.authorization_key == [0_u8; 32]
            || config.maximum_connections == 0
            || config.maximum_connections > 64
            || config.maximum_pending_connections > 1_024
        {
            return Err(HarnessDependencyError::InvalidConfiguration);
        }
        let maximum_connections = config.maximum_connections;
        Ok(Self {
            config: Arc::new(config),
            connections: Arc::new(Mutex::new(Vec::new())),
            connection_capacity: Arc::new(Semaphore::new(maximum_connections)),
            pending_connections: Arc::new(AtomicUsize::new(0)),
            cancellations: Arc::new(Mutex::new(CancellationRegistry::default())),
        })
    }
    fn connect(&self) -> Result<Connection, HarnessDependencyError> {
        let mut c = Command::new(&self.config.program);
        c.args(&self.config.arguments)
            .env_clear()
            .env(
                "AGENTMOD_HARNESS_AUTH_KEY",
                encode_hex(&self.config.authorization_key),
            )
            .env(
                "AGENTMOD_HARNESS_FRAME_PACING_MS",
                self.config.frame_pacing.as_millis().to_string(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(root) = &self.config.test_gate_root {
            c.env("AGENTMOD_HARNESS_TEST_GATE_ROOT", root);
        }
        // Provider configuration and secret references are forwarded from the
        // runtime process environment through the curated `AGENTMOD_PROVIDER_*`
        // namespace only. The harness resolves secret values itself from
        // environment references or `file:` references; secrets never cross
        // protocol frames, events, logs, or request options.
        for (name, value) in std::env::vars() {
            if name.starts_with("AGENTMOD_PROVIDER_") {
                c.env(name, value);
            }
        }
        let mut child = c.spawn().map_err(|_| HarnessDependencyError::Unavailable)?;
        let stdin = child
            .stdin
            .take()
            .ok_or(HarnessDependencyError::Unavailable)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(HarnessDependencyError::Unavailable)?;
        Ok(Connection {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    async fn acquire_connection_permit(
        &self,
    ) -> Result<OwnedSemaphorePermit, HarnessDependencyError> {
        if let Ok(permit) = self.connection_capacity.clone().try_acquire_owned() {
            return Ok(permit);
        }
        self.pending_connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                (pending < self.config.maximum_pending_connections).then_some(pending + 1)
            })
            .map_err(|_| HarnessDependencyError::TooManyPendingConnections)?;
        let permit = timeout(
            self.config.request_timeout,
            self.connection_capacity.clone().acquire_owned(),
        )
        .await;
        self.pending_connections.fetch_sub(1, Ordering::AcqRel);
        permit
            .map_err(|_| HarnessDependencyError::Timeout)?
            .map_err(|_| HarnessDependencyError::Unavailable)
    }

    async fn take_connection(&self) -> Result<Connection, HarnessDependencyError> {
        if let Some(connection) = self.connections.lock().await.pop() {
            Ok(connection)
        } else {
            self.connect()
        }
    }

    async fn release_connection(&self, connection: Connection) {
        self.connections.lock().await.push(connection);
    }

    async fn discard_connection(mut connection: Connection) {
        let _ = connection.child.start_kill();
        let _ = connection.child.wait().await;
    }

    async fn once(
        &self,
        command: &DependencyCommand,
        cancellation: Option<CancellationToken>,
    ) -> Result<DependencyReply, HarnessDependencyError> {
        let mut bytes = serde_json::to_vec(&self.to_wire(command)?)
            .map_err(|_| HarnessDependencyError::Protocol)?;
        bytes.push(b'\n');
        let _permit = self.acquire_connection_permit().await?;
        let mut connection = self.take_connection().await?;
        let result = timeout(self.config.request_timeout, async {
            connection
                .stdin
                .write_all(&bytes)
                .await
                .map_err(|_| HarnessDependencyError::Transport)?;
            connection
                .stdin
                .flush()
                .await
                .map_err(|_| HarnessDependencyError::Transport)?;
            let mut events = Vec::new();
            loop {
                let reply = if let Some(cancellation) = &cancellation {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => {
                            events.push(DependencyEvent::Cancelled);
                            return Ok(DependencyReply::Events(events));
                        }
                        reply = read_frame(&mut connection.stdout, self.config.maximum_frame_bytes) => reply?,
                    }
                } else {
                    read_frame(&mut connection.stdout, self.config.maximum_frame_bytes).await?
                };
                let reply: wire::HarnessReply =
                    serde_json::from_slice(&reply).map_err(|_| HarnessDependencyError::Protocol)?;
                match reply {
                    wire::HarnessReply::Event { event, terminal } => {
                        events.push(map_event(event));
                        if terminal {
                            return Ok(DependencyReply::Events(events));
                        }
                    }
                    reply if events.is_empty() => return Ok(from_wire(reply)),
                    _ => return Err(HarnessDependencyError::Protocol),
                }
            }
        })
        .await
        .map_err(|_| HarnessDependencyError::Timeout)?;
        let cancelled = matches!(
            &result,
            Ok(DependencyReply::Events(events))
                if matches!(events.last(), Some(DependencyEvent::Cancelled))
        );
        if result.is_ok() && !cancelled {
            self.release_connection(connection).await;
        } else {
            Self::discard_connection(connection).await;
        }
        result
    }

    async fn stream_once(
        &self,
        command: &DependencyCommand,
        cancellation: Option<CancellationToken>,
        sender: &mpsc::Sender<Result<DependencyEvent, HarnessDependencyError>>,
    ) -> Result<(), HarnessDependencyError> {
        let mut bytes = serde_json::to_vec(&self.to_wire(command)?)
            .map_err(|_| HarnessDependencyError::Protocol)?;
        bytes.push(b'\n');
        let _permit = self.acquire_connection_permit().await?;
        let mut connection = self.take_connection().await?;
        let result = timeout(self.config.request_timeout, async {
            connection
                .stdin
                .write_all(&bytes)
                .await
                .map_err(|_| HarnessDependencyError::Transport)?;
            connection
                .stdin
                .flush()
                .await
                .map_err(|_| HarnessDependencyError::Transport)?;
            loop {
                let frame = if let Some(cancellation) = &cancellation {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => {
                            sender.send(Ok(DependencyEvent::Cancelled)).await
                                .map_err(|_| HarnessDependencyError::Transport)?;
                            return Ok(());
                        }
                        reply = read_frame(
                            &mut connection.stdout,
                            self.config.maximum_frame_bytes,
                        ) => reply?,
                    }
                } else {
                    read_frame(&mut connection.stdout, self.config.maximum_frame_bytes).await?
                };
                let reply: wire::HarnessReply =
                    serde_json::from_slice(&frame).map_err(|_| HarnessDependencyError::Protocol)?;
                match reply {
                    wire::HarnessReply::Event { event, terminal } => {
                        sender
                            .send(Ok(map_event(event)))
                            .await
                            .map_err(|_| HarnessDependencyError::Transport)?;
                        if terminal {
                            return Ok(());
                        }
                    }
                    wire::HarnessReply::Events { events } => {
                        for event in events {
                            sender
                                .send(Ok(map_event(event)))
                                .await
                                .map_err(|_| HarnessDependencyError::Transport)?;
                        }
                        return Ok(());
                    }
                    wire::HarnessReply::Failed {
                        code,
                        message,
                        retryable,
                    } => {
                        sender
                            .send(Ok(DependencyEvent::Failed {
                                code,
                                message,
                                retryable,
                            }))
                            .await
                            .map_err(|_| HarnessDependencyError::Transport)?;
                        return Ok(());
                    }
                    wire::HarnessReply::Health { .. } | wire::HarnessReply::Catalog { .. } => {
                        return Err(HarnessDependencyError::Protocol);
                    }
                }
            }
        })
        .await
        .map_err(|_| HarnessDependencyError::Timeout)?;
        let cancelled = cancellation.is_some_and(|token| token.is_cancelled());
        if result.is_ok() && !cancelled {
            self.release_connection(connection).await;
        } else {
            Self::discard_connection(connection).await;
        }
        result
    }

    fn to_wire(
        &self,
        command: &DependencyCommand,
    ) -> Result<wire::HarnessCommand, HarnessDependencyError> {
        to_wire(command, &self.config.authorization_key)
    }
}
#[async_trait]
impl HarnessDependencyPort for ProcessHarnessDependency {
    async fn exchange(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyReply, HarnessDependencyError> {
        if let DependencyCommand::Cancel {
            cancellation_id, ..
        } = &command
        {
            if cancellation_id.trim().is_empty() {
                return Err(HarnessDependencyError::InvalidRequest);
            }
            let mut cancellations = self.cancellations.lock().await;
            if let Some(cancellation) = cancellations.active.get(cancellation_id) {
                cancellation.cancel();
            } else {
                if cancellations.pending.len() >= MAX_PENDING_CANCELLATIONS
                    && !cancellations.pending.contains(cancellation_id)
                {
                    return Err(HarnessDependencyError::TooManyPendingCancellations);
                }
                cancellations.pending.insert(cancellation_id.clone());
            }
            return Ok(DependencyReply::Events(vec![DependencyEvent::Cancelled]));
        }
        let active_id = match &command {
            DependencyCommand::Execute {
                cancellation_id, ..
            } => Some(cancellation_id.clone()),
            _ => None,
        };
        let cancellation = if let Some(id) = &active_id {
            let mut cancellations = self.cancellations.lock().await;
            if cancellations.pending.remove(id) {
                return Ok(DependencyReply::Events(vec![DependencyEvent::Cancelled]));
            }
            let cancellation = CancellationToken::new();
            if cancellations
                .active
                .insert(id.clone(), cancellation.clone())
                .is_some()
            {
                return Err(HarnessDependencyError::DuplicateCancellation);
            }
            Some(cancellation)
        } else {
            None
        };
        let result = self.once(&command, cancellation).await;
        if let Some(id) = active_id {
            self.cancellations.lock().await.active.remove(&id);
        }
        let was_cancelled = matches!(
            &result,
            Ok(DependencyReply::Events(events))
                if matches!(events.last(), Some(DependencyEvent::Cancelled))
        );
        if result.is_err() {
            // A failed exchange may have reached the child before its response
            // was lost. Replaying it here could duplicate a provider request.
            // Drop the desynchronized child and leave business-level,
            // idempotency-aware retry decisions to runtime logic.
            self.shutdown().await;
        } else if was_cancelled {
            self.shutdown().await;
        }
        result
    }

    async fn exchange_events(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyEventStream, HarnessDependencyError> {
        if matches!(
            command,
            DependencyCommand::Health { .. } | DependencyCommand::Cancel { .. }
        ) {
            return Err(HarnessDependencyError::InvalidRequest);
        }
        let active_id = match &command {
            DependencyCommand::Execute {
                cancellation_id, ..
            } => Some(cancellation_id.clone()),
            DependencyCommand::Continue { .. } => None,
            DependencyCommand::Cancel { .. } | DependencyCommand::Health { .. } => unreachable!(),
        };
        let cancellation = if let Some(id) = &active_id {
            let mut cancellations = self.cancellations.lock().await;
            if cancellations.pending.remove(id) {
                let (sender, receiver) = mpsc::channel(1);
                sender
                    .try_send(Ok(DependencyEvent::Cancelled))
                    .map_err(|_| HarnessDependencyError::Transport)?;
                return Ok(DependencyEventStream { receiver });
            }
            let cancellation = CancellationToken::new();
            if cancellations
                .active
                .insert(id.clone(), cancellation.clone())
                .is_some()
            {
                return Err(HarnessDependencyError::DuplicateCancellation);
            }
            Some(cancellation)
        } else {
            None
        };
        let (sender, receiver) = mpsc::channel(16);
        let adapter = self.clone();
        tokio::spawn(async move {
            let result = adapter
                .stream_once(&command, cancellation.clone(), &sender)
                .await;
            let was_cancelled = cancellation.is_some_and(|token| token.is_cancelled());
            if let Some(id) = active_id {
                adapter.cancellations.lock().await.active.remove(&id);
            }
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
                adapter.shutdown().await;
            } else if was_cancelled {
                adapter.shutdown().await;
            }
        });
        Ok(DependencyEventStream { receiver })
    }

    async fn shutdown(&self) {
        let connections = std::mem::take(&mut *self.connections.lock().await);
        for mut c in connections {
            let _ = c.child.start_kill();
            let _ = c.child.wait().await;
        }
    }
}
fn to_wire(
    v: &DependencyCommand,
    authorization_key: &[u8; 32],
) -> Result<wire::HarnessCommand, HarnessDependencyError> {
    Ok(match v {
        DependencyCommand::Health { .. } => wire::HarnessCommand::Health,
        DependencyCommand::Cancel {
            cancellation_id, ..
        } => wire::HarnessCommand::Cancel {
            cancellation_id: cancellation_id
                .parse()
                .map_err(|_| HarnessDependencyError::Protocol)?,
        },
        DependencyCommand::Continue {
            continuation_id,
            decision,
            ..
        } => wire::HarnessCommand::Continue {
            continuation_id: continuation_id
                .parse()
                .map_err(|_| HarnessDependencyError::Protocol)?,
            decision: match decision {
                DependencyDecision::Continue => wire::HarnessContinuationDecision::Continue,
                DependencyDecision::Replace(v) => {
                    wire::HarnessContinuationDecision::ReplaceContext {
                        entries: v.iter().map(map_entry).collect(),
                    }
                }
                DependencyDecision::Reject(reason) => wire::HarnessContinuationDecision::Reject {
                    reason: reason.clone(),
                },
                DependencyDecision::Cancel(reason) => wire::HarnessContinuationDecision::Cancel {
                    reason: reason.clone(),
                },
            },
        },
        DependencyCommand::Execute {
            session_id,
            provider,
            model,
            entries,
            options,
            grant,
            cancellation_id,
            ..
        } => wire::HarnessCommand::Execute {
            session_id: session_id
                .parse()
                .map_err(|_| HarnessDependencyError::Protocol)?,
            provider: provider.clone(),
            model: model.clone(),
            entries: entries.iter().map(map_entry).collect(),
            options: options.clone(),
            authorization_grant: sign_grant(grant, authorization_key)?,
            cancellation_id: cancellation_id
                .parse()
                .map_err(|_| HarnessDependencyError::Protocol)?,
        },
    })
}

fn sign_grant(binding: &str, key: &[u8; 32]) -> Result<String, HarnessDependencyError> {
    if binding.len() != 64 || !binding.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HarnessDependencyError::Protocol);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| HarnessDependencyError::Unavailable)?;
    let expires = now
        .as_millis()
        .checked_add(120_000)
        .ok_or(HarnessDependencyError::Unavailable)?;
    let nonce = uuid::Uuid::now_v7();
    let payload = format!("v1.{expires}.{nonce}.{binding}");
    let signature = blake3::keyed_hash(key, payload.as_bytes());
    Ok(format!("{payload}.{}", signature.to_hex()))
}

fn encode_hex(bytes: &[u8; 32]) -> String {
    blake3::Hash::from_bytes(*bytes).to_hex().to_string()
}
fn map_entry(v: &DependencyEntry) -> wire::ProjectedEntry {
    match v {
        DependencyEntry::System(text) => wire::ProjectedEntry::System { text: text.clone() },
        DependencyEntry::User(text) => wire::ProjectedEntry::User { text: text.clone() },
        DependencyEntry::Assistant(text) => wire::ProjectedEntry::Assistant { text: text.clone() },
        DependencyEntry::ToolCall {
            call_id,
            tool,
            arguments,
        } => wire::ProjectedEntry::ToolCall {
            call_id: call_id.clone(),
            tool: tool.clone(),
            arguments: arguments.clone(),
        },
        DependencyEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => wire::ProjectedEntry::ToolResult {
            call_id: call_id.clone(),
            content: content.clone(),
            truncated: *truncated,
        },
        DependencyEntry::Summary { text, start, end } => wire::ProjectedEntry::ContextSummary {
            text: text.clone(),
            source_start: *start,
            source_end: *end,
        },
        DependencyEntry::Metadata { key, value } => wire::ProjectedEntry::Metadata {
            key: key.clone(),
            value: value.clone(),
        },
    }
}
fn from_wire(v: wire::HarnessReply) -> DependencyReply {
    match v {
        wire::HarnessReply::Health {
            status,
            ready_provider_count,
            capabilities,
        } => DependencyReply::Health {
            status,
            ready: ready_provider_count,
            capabilities,
        },
        wire::HarnessReply::Failed {
            code,
            message,
            retryable,
        } => DependencyReply::Failed {
            code,
            message,
            retryable,
        },
        wire::HarnessReply::Events { events } => {
            DependencyReply::Events(events.into_iter().map(map_event).collect())
        }
        wire::HarnessReply::Event { event, .. } => DependencyReply::Events(vec![map_event(event)]),
        wire::HarnessReply::Catalog { .. } => DependencyReply::Events(Vec::new()),
    }
}
fn map_event(v: wire::HarnessEvent) -> DependencyEvent {
    match v {
        wire::HarnessEvent::Started => DependencyEvent::Started,
        wire::HarnessEvent::TextDelta { text } => DependencyEvent::Text(text),
        wire::HarnessEvent::ToolCallDelta {
            call_id,
            name_fragment,
            arguments_fragment,
        } => DependencyEvent::ToolDelta {
            call_id,
            name: name_fragment,
            arguments: arguments_fragment,
        },
        wire::HarnessEvent::ToolCallProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => DependencyEvent::ToolProposed {
            continuation_id: continuation_id.to_string(),
            call_id,
            tool,
            arguments,
        },
        wire::HarnessEvent::Completed {
            finish_reason,
            usage,
        } => DependencyEvent::Completed {
            reason: finish_reason,
            usage: DependencyUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cache_read_tokens: usage.cache_read_tokens,
                cache_write_tokens: usage.cache_write_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                estimated: usage.estimated,
                cost: usage.cost.map(|cost| CostMetadata {
                    source: cost.source,
                    version: cost.version,
                    input_cost_micros: cost.input_cost_micros,
                    output_cost_micros: cost.output_cost_micros,
                    cache_read_cost_micros: cost.cache_read_cost_micros,
                    cache_write_cost_micros: cost.cache_write_cost_micros,
                    currency: cost.currency,
                }),
            },
        },
        wire::HarnessEvent::Cancelled => DependencyEvent::Cancelled,
        wire::HarnessEvent::Failed {
            code,
            message,
            retryable,
        } => DependencyEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}
async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> Result<Vec<u8>, HarnessDependencyError> {
    let mut bytes = Vec::new();
    loop {
        match reader.read_u8().await {
            Ok(b'\n') => return Ok(bytes),
            Ok(byte) if bytes.len() < maximum => bytes.push(byte),
            Ok(_) => return Err(HarnessDependencyError::FrameTooLarge),
            Err(_) => return Err(HarnessDependencyError::Transport),
        }
    }
}
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HarnessDependencyError {
    #[error("invalid harness dependency configuration")]
    InvalidConfiguration,
    #[error("harness unavailable")]
    Unavailable,
    #[error("harness transport failed")]
    Transport,
    #[error("harness protocol failed")]
    Protocol,
    #[error("harness reply too large")]
    FrameTooLarge,
    #[error("harness request timed out")]
    Timeout,
    #[error("harness cancellation identifier is already active")]
    DuplicateCancellation,
    #[error("harness request is invalid")]
    InvalidRequest,
    #[error("harness cancellation identifier is not active")]
    UnknownCancellation,
    #[error("harness pending cancellation capacity is exhausted")]
    TooManyPendingCancellations,
    #[error("harness pending connection capacity is exhausted")]
    TooManyPendingConnections,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dependency() -> ProcessHarnessDependency {
        ProcessHarnessDependency::new(HarnessDependencyConfig {
            program: String::from("must-not-start-for-pre-cancel"),
            arguments: Vec::new(),
            maximum_frame_bytes: 1024,
            request_timeout: Duration::from_secs(1),
            frame_pacing: Duration::ZERO,
            authorization_key: [7_u8; 32],
            maximum_connections: 1,
            maximum_pending_connections: 1,
            test_gate_root: None,
        })
        .expect("dependency")
    }

    #[tokio::test]
    async fn cancellation_before_execute_is_latched_without_spawning() {
        let dependency = dependency();
        assert_eq!(
            dependency
                .exchange(DependencyCommand::Cancel {
                    harness_id: String::from("native"),
                    cancellation_id: String::from("cancel-before-start"),
                })
                .await
                .expect("pre-cancel"),
            DependencyReply::Events(vec![DependencyEvent::Cancelled])
        );
        let mut stream = dependency
            .exchange_events(DependencyCommand::Execute {
                harness_id: String::from("native"),
                session_id: String::from("session"),
                provider: String::from("provider"),
                model: String::from("model"),
                entries: Vec::new(),
                options: Value::Object(serde_json::Map::default()),
                grant: String::from("unused"),
                cancellation_id: String::from("cancel-before-start"),
            })
            .await
            .expect("cancelled execution stream");
        assert_eq!(
            stream.next().await.expect("event").expect("valid event"),
            DependencyEvent::Cancelled
        );
        assert!(stream.next().await.is_none());
        assert!(dependency.connections.lock().await.is_empty());
    }

    #[tokio::test]
    async fn connection_admission_is_bounded_and_applies_backpressure() {
        let dependency = dependency();
        let active = dependency
            .acquire_connection_permit()
            .await
            .expect("active connection permit");
        let waiting_dependency = dependency.clone();
        let waiting = tokio::spawn(async move {
            waiting_dependency
                .acquire_connection_permit()
                .await
                .expect("queued connection permit")
        });
        while dependency.pending_connections.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            dependency.acquire_connection_permit().await,
            Err(HarnessDependencyError::TooManyPendingConnections)
        ));
        drop(active);
        drop(waiting.await.expect("queued task"));
        assert_eq!(dependency.pending_connections.load(Ordering::Acquire), 0);
    }
}
