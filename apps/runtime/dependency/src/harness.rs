//! Supervised local harness-process adapter.
#![allow(
    missing_docs,
    reason = "dependency-local transport records are self-describing"
)]
use agentmod_harness_protocol as wire;
use async_trait::async_trait;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc},
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
        session_id: String,
        provider: String,
        model: String,
        entries: Vec<DependencyEntry>,
        options: Value,
        grant: String,
        cancellation_id: String,
    },
    Continue {
        continuation_id: String,
        decision: DependencyDecision,
    },
    Cancel {
        cancellation_id: String,
    },
    Health,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
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
}
#[derive(Clone, Debug)]
pub struct HarnessDependencyConfig {
    pub program: String,
    pub arguments: Vec<String>,
    pub maximum_frame_bytes: usize,
    pub request_timeout: Duration,
    pub frame_pacing: Duration,
    pub authorization_key: [u8; 32],
}
struct Connection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}
#[derive(Clone)]
pub struct ProcessHarnessDependency {
    config: Arc<HarnessDependencyConfig>,
    connection: Arc<Mutex<Option<Connection>>>,
    active: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
}
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
        {
            return Err(HarnessDependencyError::InvalidConfiguration);
        }
        Ok(Self {
            config: Arc::new(config),
            connection: Arc::new(Mutex::new(None)),
            active: Arc::new(Mutex::new(BTreeMap::new())),
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
    async fn once(
        &self,
        command: &DependencyCommand,
        cancellation: Option<CancellationToken>,
    ) -> Result<DependencyReply, HarnessDependencyError> {
        let mut guard = self.connection.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect()?);
        }
        let c = guard.as_mut().ok_or(HarnessDependencyError::Unavailable)?;
        let mut bytes = serde_json::to_vec(&self.to_wire(command)?)
            .map_err(|_| HarnessDependencyError::Protocol)?;
        bytes.push(b'\n');
        c.stdin
            .write_all(&bytes)
            .await
            .map_err(|_| HarnessDependencyError::Transport)?;
        c.stdin
            .flush()
            .await
            .map_err(|_| HarnessDependencyError::Transport)?;
        timeout(self.config.request_timeout, async {
            let mut events = Vec::new();
            loop {
                let reply = if let Some(cancellation) = &cancellation {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => {
                            events.push(DependencyEvent::Cancelled);
                            return Ok(DependencyReply::Events(events));
                        }
                        reply = read_frame(&mut c.stdout, self.config.maximum_frame_bytes) => reply?,
                    }
                } else {
                    read_frame(&mut c.stdout, self.config.maximum_frame_bytes).await?
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
        .map_err(|_| HarnessDependencyError::Timeout)?
    }

    async fn stream_once(
        &self,
        command: &DependencyCommand,
        cancellation: Option<CancellationToken>,
        sender: &mpsc::Sender<Result<DependencyEvent, HarnessDependencyError>>,
    ) -> Result<(), HarnessDependencyError> {
        let mut guard = self.connection.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect()?);
        }
        let connection = guard.as_mut().ok_or(HarnessDependencyError::Unavailable)?;
        let mut bytes = serde_json::to_vec(&self.to_wire(command)?)
            .map_err(|_| HarnessDependencyError::Protocol)?;
        bytes.push(b'\n');
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
        timeout(self.config.request_timeout, async {
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
                    wire::HarnessReply::Health { .. } => {
                        return Err(HarnessDependencyError::Protocol);
                    }
                }
            }
        })
        .await
        .map_err(|_| HarnessDependencyError::Timeout)?
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
        if let DependencyCommand::Cancel { cancellation_id } = &command {
            if cancellation_id.trim().is_empty() {
                return Err(HarnessDependencyError::InvalidRequest);
            }
            let cancellation = self
                .active
                .lock()
                .await
                .get(cancellation_id)
                .cloned()
                .ok_or(HarnessDependencyError::UnknownCancellation)?;
            cancellation.cancel();
            return Ok(DependencyReply::Events(vec![DependencyEvent::Cancelled]));
        }
        let active_id = match &command {
            DependencyCommand::Execute {
                cancellation_id, ..
            } => Some(cancellation_id.clone()),
            _ => None,
        };
        let cancellation = if let Some(id) = &active_id {
            let cancellation = CancellationToken::new();
            if self
                .active
                .lock()
                .await
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
            self.active.lock().await.remove(&id);
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
            DependencyCommand::Health | DependencyCommand::Cancel { .. }
        ) {
            return Err(HarnessDependencyError::InvalidRequest);
        }
        let active_id = match &command {
            DependencyCommand::Execute {
                cancellation_id, ..
            } => Some(cancellation_id.clone()),
            DependencyCommand::Continue { .. } => None,
            DependencyCommand::Cancel { .. } | DependencyCommand::Health => unreachable!(),
        };
        let cancellation = if let Some(id) = &active_id {
            let cancellation = CancellationToken::new();
            if self
                .active
                .lock()
                .await
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
                adapter.active.lock().await.remove(&id);
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
        if let Some(mut c) = self.connection.lock().await.take() {
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
        DependencyCommand::Health => wire::HarnessCommand::Health,
        DependencyCommand::Cancel { cancellation_id } => wire::HarnessCommand::Cancel {
            cancellation_id: cancellation_id
                .parse()
                .map_err(|_| HarnessDependencyError::Protocol)?,
        },
        DependencyCommand::Continue {
            continuation_id,
            decision,
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
}
