//! Supervised process capability-host adapter.
#![allow(
    missing_docs,
    reason = "dependency-local process transport records are self-describing"
)]

use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{
    CancellationId, CausationId, ContentHash, CorrelationId, IdempotencyId, RequestId,
    TimestampMillis,
};
use agentmod_protocol_support::{
    FrameHeader, FrameKind, Handshake, Negotiated, WireFrame,
    authorization::{AuthorizationClaims, AuthorizationKey, seal_authorization},
    read_frame as read_protocol_frame, write_frame as write_protocol_frame,
};
use agentmod_tool_protocol::{OutputStream, PROTOCOL_VERSION, ToolHostCommand, ToolHostEvent};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    process::Command,
    sync::Mutex,
    time::{sleep, timeout},
};

use crate::tool::{
    DependencyCancelToolRequest, DependencyOutputStream, DependencyToolCommand,
    DependencyToolEvent, ToolHostDependencyError, ToolHostDependencyPort,
};

#[derive(Clone, Debug)]
pub struct ProcessCapabilityDependencyConfig {
    pub program: String,
    pub arguments: Vec<String>,
    pub owner: String,
    pub allowed_executables: Vec<String>,
    pub endpoint_root: PathBuf,
    pub host_idle_timeout: Duration,
    pub maximum_frame_bytes: usize,
    pub request_timeout: Duration,
    pub authorization_key: [u8; 32],
}

trait ProcessLocalStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ProcessLocalStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

struct Connection {
    stream: Box<dyn ProcessLocalStream>,
}

#[derive(Clone)]
pub struct ProcessCapabilityDependency {
    config: Arc<ProcessCapabilityDependencyConfig>,
    connections: Arc<Mutex<HashMap<String, Connection>>>,
}

impl ProcessCapabilityDependency {
    /// Generates a runtime-lifetime process-host trust key.
    #[must_use]
    pub fn generate_authorization_key() -> [u8; 32] {
        let first = uuid::Uuid::now_v7();
        let second = uuid::Uuid::now_v7();
        let mut key = [0; 32];
        key[..16].copy_from_slice(first.as_bytes());
        key[16..].copy_from_slice(second.as_bytes());
        key
    }

    /// Derives a restart-stable host trust key from a protected bootstrap
    /// secret.
    #[must_use]
    pub fn derive_authorization_key(seed: &[u8]) -> [u8; 32] {
        let mut material = Vec::with_capacity(38 + seed.len());
        material.extend_from_slice(b"agentmod.process-host.authorization.v1\0");
        material.extend_from_slice(seed);
        *blake3::hash(&material).as_bytes()
    }

    /// Creates a lazy per-session process-host supervisor.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe process, transport, identity, or key
    /// configuration.
    pub fn new(config: ProcessCapabilityDependencyConfig) -> Result<Self, ToolHostDependencyError> {
        if config.program.trim().is_empty()
            || config.program.contains('\0')
            || config.arguments.iter().any(|value| value.contains('\0'))
            || config.owner.trim().is_empty()
            || !config.endpoint_root.is_absolute()
            || config.host_idle_timeout.is_zero()
            || config.host_idle_timeout > Duration::from_secs(24 * 60 * 60)
            || config.maximum_frame_bytes == 0
            || config.request_timeout.is_zero()
            || config.authorization_key == [0; 32]
        {
            return Err(ToolHostDependencyError::InvalidConfiguration);
        }
        prepare_endpoint_root(&config.endpoint_root)?;
        Ok(Self {
            config: Arc::new(config),
            connections: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn connect(
        &self,
        session: &str,
        workspace: &Path,
    ) -> Result<Connection, ToolHostDependencyError> {
        let endpoint = self.endpoint(session, workspace);
        match open_endpoint(&endpoint).await {
            Ok(mut stream) => {
                self.handshake(&mut stream).await?;
                return Ok(Connection { stream });
            }
            Err(_) => self.spawn_host(session, workspace, &endpoint)?,
        }
        for _ in 0..100 {
            match open_endpoint(&endpoint).await {
                Ok(mut stream) => {
                    self.handshake(&mut stream).await?;
                    return Ok(Connection { stream });
                }
                Err(_) => sleep(Duration::from_millis(20)).await,
            }
        }
        Err(ToolHostDependencyError::Unavailable)
    }

    async fn connect_existing(
        &self,
        session: &str,
        workspace: &Path,
    ) -> Result<Connection, ToolHostDependencyError> {
        let endpoint = self.endpoint(session, workspace);
        let mut stream = open_endpoint(&endpoint)
            .await
            .map_err(|_| ToolHostDependencyError::Unavailable)?;
        self.handshake(&mut stream).await?;
        Ok(Connection { stream })
    }

    /// Cancels an exact active process-host request through an independent
    /// authenticated connection.
    ///
    /// # Errors
    ///
    /// Returns a dependency-owned error for invalid identifiers, unavailable
    /// endpoints, failed authentication, or malformed terminal responses.
    pub async fn cancel_active(
        &self,
        session: &str,
        workspace: &Path,
        cancellation_id: &str,
    ) -> Result<bool, ToolHostDependencyError> {
        let cancellation_id = CancellationId::from_str(cancellation_id)
            .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
        let mut connection = None;
        for _ in 0..20 {
            match self.connect_existing(session, workspace).await {
                Ok(value) => {
                    connection = Some(value);
                    break;
                }
                Err(ToolHostDependencyError::Unavailable) => {
                    sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        let mut connection = connection.ok_or(ToolHostDependencyError::Unavailable)?;
        let events = self
            .send_command(
                &mut connection,
                ToolHostCommand::Cancel { cancellation_id },
                cancellation_id,
            )
            .await?;
        Ok(events
            .iter()
            .any(|event| matches!(event, DependencyToolEvent::Cancelled { .. })))
    }

    fn spawn_host(
        &self,
        session: &str,
        workspace: &Path,
        endpoint: &str,
    ) -> Result<(), ToolHostDependencyError> {
        let mut command = Command::new(&self.config.program);
        command
            .args(&self.config.arguments)
            .current_dir(workspace)
            .env_clear()
            .env("AGENTMOD_PROCESS_OWNER", &self.config.owner)
            .env("AGENTMOD_PROCESS_SESSION", session)
            .env(
                "AGENTMOD_PROCESS_AUTH_KEY",
                encode_hex(&self.config.authorization_key),
            )
            .env(
                "AGENTMOD_PROCESS_ALLOWED_EXECUTABLES",
                self.config.allowed_executables.join(";"),
            )
            .env("AGENTMOD_PROCESS_ENDPOINT", endpoint)
            .env(
                "AGENTMOD_PROCESS_IDLE_TIMEOUT_MS",
                self.config.host_idle_timeout.as_millis().to_string(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        configure_detached_host(&mut command);
        for name in [
            "ALLUSERSPROFILE",
            "APPDATA",
            "CARGO_HOME",
            "CommonProgramFiles",
            "CommonProgramFiles(x86)",
            "CommonProgramW6432",
            "ComSpec",
            "HOME",
            "HOMEDRIVE",
            "HOMEPATH",
            "LOCALAPPDATA",
            "NUMBER_OF_PROCESSORS",
            "PATH",
            "PATHEXT",
            "PROCESSOR_ARCHITECTURE",
            "ProgramData",
            "ProgramFiles",
            "ProgramFiles(x86)",
            "ProgramW6432",
            "RUSTUP_HOME",
            "SystemDrive",
            "SYSTEMROOT",
            "TEMP",
            "TMP",
            "USERNAME",
            "USERPROFILE",
            "WINDIR",
        ] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        let mut child = command
            .spawn()
            .map_err(|_| ToolHostDependencyError::Unavailable)?;
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        Ok(())
    }

    async fn handshake(
        &self,
        stream: &mut Box<dyn ProcessLocalStream>,
    ) -> Result<(), ToolHostDependencyError> {
        let request = new_header(FrameKind::Handshake, None);
        write_protocol_frame(
            stream,
            &WireFrame {
                header: request.clone(),
                payload: Handshake {
                    supported_versions: vec![PROTOCOL_VERSION],
                    capabilities: std::collections::BTreeSet::from([
                        String::from("bounded_backpressure"),
                        String::from("cancellation"),
                        String::from("idempotency"),
                        String::from("request_response"),
                        String::from("streaming"),
                    ]),
                    authorization_token: encode_hex(&self.config.authorization_key),
                },
            },
            self.config.maximum_frame_bytes,
        )
        .await
        .map_err(|_| ToolHostDependencyError::Transport)?;
        let response: WireFrame<Negotiated> =
            read_protocol_frame(stream, self.config.maximum_frame_bytes)
                .await
                .map_err(|_| ToolHostDependencyError::Transport)?;
        validate_unary_header(&response.header, &request)?;
        if !response
            .payload
            .version
            .is_compatible_with(PROTOCOL_VERSION)
            || !response.payload.capabilities.contains("request_response")
            || !response.payload.capabilities.contains("streaming")
        {
            return Err(ToolHostDependencyError::Protocol);
        }
        Ok(())
    }

    fn endpoint(&self, session: &str, workspace: &Path) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"agentmod.process-host.endpoint.v1\0");
        hasher.update(self.config.owner.as_bytes());
        hasher.update(b"\0");
        hasher.update(session.as_bytes());
        hasher.update(b"\0");
        hasher.update(workspace.as_os_str().to_string_lossy().as_bytes());
        let digest = hasher.finalize().to_hex();
        endpoint_name(&self.config.endpoint_root, &digest)
    }

    async fn send_command(
        &self,
        connection: &mut Connection,
        wire: ToolHostCommand,
        cancellation_id: CancellationId,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError> {
        let request = new_header(FrameKind::Request, Some(cancellation_id));
        write_protocol_frame(
            &mut connection.stream,
            &WireFrame {
                header: request.clone(),
                payload: wire,
            },
            self.config.maximum_frame_bytes,
        )
        .await
        .map_err(|_| ToolHostDependencyError::Transport)?;
        let mut events = Vec::new();
        let mut sequence = 1_u64;
        loop {
            let response: WireFrame<ToolHostEvent> =
                read_protocol_frame(&mut connection.stream, self.config.maximum_frame_bytes)
                    .await
                    .map_err(|_| ToolHostDependencyError::Transport)?;
            validate_event_header(&response.header, &request, sequence)?;
            let terminal_event = matches!(
                response.payload,
                ToolHostEvent::Completed { .. }
                    | ToolHostEvent::Failed { .. }
                    | ToolHostEvent::Cancelled { .. }
            );
            let terminal_frame = matches!(
                response.header.kind,
                FrameKind::Response | FrameKind::StreamEnd
            );
            if terminal_event != terminal_frame {
                return Err(ToolHostDependencyError::Protocol);
            }
            events.push(map_event(response.payload)?);
            if terminal_frame {
                return Ok(events);
            }
            sequence = sequence
                .checked_add(1)
                .ok_or(ToolHostDependencyError::Protocol)?;
        }
    }

    async fn execute_once(
        &self,
        command: DependencyToolCommand,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError> {
        crate::tool::validate(&command)?;
        validate(&command)?;
        if !command.tool.starts_with("process.") {
            return Err(ToolHostDependencyError::UnsupportedTool);
        }
        let cancellation_id = CancellationId::from_str(&command.cancellation_id)
            .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
        let operation =
            canonical_operation(&command.tool, &command.arguments, &command.cancellation_id)?;
        let digest = ContentHash::digest(&operation);
        let grant = self.grant(&command, digest)?;
        let wire = ToolHostCommand::Execute {
            call_id: command.call_id.clone(),
            tool: command.tool.clone(),
            arguments: command.arguments,
            normalized_digest: digest.to_hex(),
            authorization_grant: grant,
            cancellation_id,
        };
        let key = connection_key(&command.session_id, &command.workspace);
        let mut connections = self.connections.lock().await;
        if !connections.contains_key(&key) {
            connections.insert(
                key.clone(),
                self.connect(&command.session_id, &command.workspace)
                    .await?,
            );
        }
        let connection = connections
            .get_mut(&key)
            .ok_or(ToolHostDependencyError::Unavailable)?;
        self.send_command(connection, wire, cancellation_id).await
    }

    fn grant(
        &self,
        command: &DependencyToolCommand,
        digest: ContentHash,
    ) -> Result<String, ToolHostDependencyError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ToolHostDependencyError::Clock)?;
        let issued_at =
            i64::try_from(now.as_millis()).map_err(|_| ToolHostDependencyError::Clock)?;
        seal_authorization(
            &AuthorizationClaims {
                owner: self.config.owner.clone(),
                session: command.session_id.clone(),
                call_id: command.call_id.clone(),
                action: command.tool.clone(),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(issued_at),
                expires_at: TimestampMillis::new(issued_at + 30_000),
                nonce: uuid::Uuid::now_v7().to_string(),
            },
            &AuthorizationKey::from_bytes(self.config.authorization_key),
        )
        .map_err(|_| ToolHostDependencyError::Authorization)
    }
}

#[async_trait]
impl ToolHostDependencyPort for ProcessCapabilityDependency {
    async fn execute(
        &self,
        command: DependencyToolCommand,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError> {
        let key = connection_key(&command.session_id, &command.workspace);
        let result = match timeout(self.config.request_timeout, self.execute_once(command)).await {
            Ok(result) => result,
            Err(_) => Err(ToolHostDependencyError::Timeout),
        };
        if result.is_err() {
            self.connections.lock().await.remove(&key);
        }
        result
    }

    async fn cancel(
        &self,
        _request: DependencyCancelToolRequest,
    ) -> Result<bool, ToolHostDependencyError> {
        Ok(false)
    }

    async fn shutdown(&self) {
        self.connections.lock().await.clear();
    }
}

fn prepare_endpoint_root(root: &Path) -> Result<(), ToolHostDependencyError> {
    std::fs::create_dir_all(root).map_err(|_| ToolHostDependencyError::Unavailable)?;
    let metadata =
        std::fs::symlink_metadata(root).map_err(|_| ToolHostDependencyError::Unavailable)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ToolHostDependencyError::InvalidConfiguration);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| ToolHostDependencyError::Unavailable)?;
    }
    Ok(())
}

#[cfg(unix)]
fn endpoint_name(root: &Path, digest: &str) -> String {
    root.join(format!("process-{}.sock", &digest[..32]))
        .to_string_lossy()
        .into_owned()
}

#[cfg(windows)]
fn endpoint_name(_root: &Path, digest: &str) -> String {
    format!(r"\\.\pipe\agentmod-process-{}", &digest[..32])
}

#[cfg(unix)]
async fn open_endpoint(endpoint: &str) -> io::Result<Box<dyn ProcessLocalStream>> {
    tokio::net::UnixStream::connect(endpoint)
        .await
        .map(|stream| Box::new(stream) as Box<dyn ProcessLocalStream>)
}

#[cfg(windows)]
async fn open_endpoint(endpoint: &str) -> io::Result<Box<dyn ProcessLocalStream>> {
    tokio::task::yield_now().await;
    tokio::net::windows::named_pipe::ClientOptions::new()
        .open(endpoint)
        .map(|stream| Box::new(stream) as Box<dyn ProcessLocalStream>)
}

#[cfg(unix)]
fn configure_detached_host(command: &mut Command) {
    command.process_group(0);
}

#[cfg(windows)]
fn configure_detached_host(command: &mut Command) {
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
}

fn new_header(kind: FrameKind, cancellation_id: Option<CancellationId>) -> FrameHeader {
    FrameHeader {
        family: String::from("tool"),
        version: PROTOCOL_VERSION,
        kind,
        request_id: RequestId::from_uuid(uuid::Uuid::now_v7()),
        stream_sequence: None,
        correlation_id: CorrelationId::from_uuid(uuid::Uuid::now_v7()),
        causation_id: CausationId::from_uuid(uuid::Uuid::now_v7()),
        idempotency_id: IdempotencyId::from_uuid(uuid::Uuid::now_v7()),
        cancellation_id,
    }
}

fn validate_unary_header(
    response: &FrameHeader,
    request: &FrameHeader,
) -> Result<(), ToolHostDependencyError> {
    if response.family != "tool"
        || response.kind != FrameKind::Response
        || response.stream_sequence != Some(1)
        || !response.version.is_compatible_with(PROTOCOL_VERSION)
        || response.request_id != request.request_id
        || response.correlation_id != request.correlation_id
        || response.causation_id != request.causation_id
        || response.idempotency_id != request.idempotency_id
        || response.cancellation_id != request.cancellation_id
    {
        return Err(ToolHostDependencyError::Protocol);
    }
    Ok(())
}

fn validate_event_header(
    response: &FrameHeader,
    request: &FrameHeader,
    expected_sequence: u64,
) -> Result<(), ToolHostDependencyError> {
    if response.family != "tool"
        || !matches!(
            response.kind,
            FrameKind::Response | FrameKind::StreamItem | FrameKind::StreamEnd
        )
        || response.stream_sequence != Some(expected_sequence)
        || (response.kind == FrameKind::Response && expected_sequence != 1)
        || !response.version.is_compatible_with(PROTOCOL_VERSION)
        || response.request_id != request.request_id
        || response.correlation_id != request.correlation_id
        || response.causation_id != request.causation_id
        || response.idempotency_id != request.idempotency_id
        || response.cancellation_id != request.cancellation_id
    {
        return Err(ToolHostDependencyError::Protocol);
    }
    Ok(())
}

fn validate(command: &DependencyToolCommand) -> Result<(), ToolHostDependencyError> {
    if command.session_id.trim().is_empty()
        || command.workspace.as_os_str().is_empty()
        || command.call_id.trim().is_empty()
        || !command.arguments.is_object()
        || command.cancellation_id.trim().is_empty()
    {
        return Err(ToolHostDependencyError::InvalidRequest);
    }
    Ok(())
}

fn canonical_operation(
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<Vec<u8>, ToolHostDependencyError> {
    let object = arguments
        .as_object()
        .ok_or(ToolHostDependencyError::InvalidRequest)?;
    let normalized = match tool {
        "process.run" | "process.start" => json!({
            "executable":string(object, "executable")?,
            "arguments":string_array(object, "arguments")?,
            "working_directory":object.get("working_directory").cloned().unwrap_or(Value::Null),
            "environment":string_map(object, "environment")?,
            "timeout_ms":object.get("timeout_ms").cloned().unwrap_or(Value::Null),
            "output_limit_bytes":u64_value(object, "output_limit_bytes")?,
            "cleanup":object.get("cleanup").and_then(Value::as_str).unwrap_or("retain"),
        }),
        "process.run_pty" | "process.start_pty" => json!({
            "executable":string(object, "executable")?,
            "arguments":string_array(object, "arguments")?,
            "working_directory":object.get("working_directory").cloned().unwrap_or(Value::Null),
            "environment":string_map(object, "environment")?,
            "timeout_ms":object.get("timeout_ms").cloned().unwrap_or(Value::Null),
            "output_limit_bytes":u64_value(object, "output_limit_bytes")?,
            "cleanup":object.get("cleanup").and_then(Value::as_str).unwrap_or("retain"),
            "terminal":terminal_size(object)?,
        }),
        "process.read" => json!({
            "process_id":string(object, "process_id")?,
            "stream":string(object, "stream")?,
            "offset":u64_value(object, "offset")?,
            "length":u64_value(object, "length")?,
        }),
        "process.input" => json!({
            "process_id":string(object, "process_id")?,
            "content":string(object, "content")?,
            "close":object.get("close").and_then(Value::as_bool).unwrap_or(false),
        }),
        "process.resize" => json!({
            "process_id":string(object, "process_id")?,
            "columns":u16_value(object, "columns")?,
            "rows":u16_value(object, "rows")?,
            "pixel_width":optional_u16_value(object, "pixel_width")?,
            "pixel_height":optional_u16_value(object, "pixel_height")?,
        }),
        "process.wait" | "process.interrupt" | "process.kill" | "process.detach"
        | "process.reattach" => json!({"process_id":string(object, "process_id")?}),
        "process.list" if object.is_empty() => json!({}),
        _ => return Err(ToolHostDependencyError::UnsupportedTool),
    };
    let sorted = normalize_json(&normalized);
    serde_json::to_vec(&(tool, cancellation_id, sorted))
        .map_err(|_| ToolHostDependencyError::Protocol)
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}

fn string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ToolHostDependencyError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ToolHostDependencyError::InvalidRequest)
}

fn u64_value(object: &Map<String, Value>, key: &str) -> Result<u64, ToolHostDependencyError> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or(ToolHostDependencyError::InvalidRequest)
}

fn u16_value(object: &Map<String, Value>, key: &str) -> Result<u16, ToolHostDependencyError> {
    u16::try_from(u64_value(object, key)?).map_err(|_| ToolHostDependencyError::InvalidRequest)
}

fn optional_u16_value(
    object: &Map<String, Value>,
    key: &str,
) -> Result<u16, ToolHostDependencyError> {
    object.get(key).map_or(Ok(0), |value| {
        value
            .as_u64()
            .ok_or(ToolHostDependencyError::InvalidRequest)
            .and_then(|value| {
                u16::try_from(value).map_err(|_| ToolHostDependencyError::InvalidRequest)
            })
    })
}

fn terminal_size(object: &Map<String, Value>) -> Result<Value, ToolHostDependencyError> {
    let terminal = object
        .get("terminal")
        .and_then(Value::as_object)
        .ok_or(ToolHostDependencyError::InvalidRequest)?;
    Ok(json!({
        "columns":u16_value(terminal, "columns")?,
        "rows":u16_value(terminal, "rows")?,
        "pixel_width":optional_u16_value(terminal, "pixel_width")?,
        "pixel_height":optional_u16_value(terminal, "pixel_height")?,
    }))
}

fn string_array(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, ToolHostDependencyError> {
    object.get(key).map_or_else(
        || Ok(Vec::new()),
        |value| {
            value
                .as_array()
                .ok_or(ToolHostDependencyError::InvalidRequest)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or(ToolHostDependencyError::InvalidRequest)
                })
                .collect()
        },
    )
}

fn string_map(
    object: &Map<String, Value>,
    key: &str,
) -> Result<BTreeMap<String, String>, ToolHostDependencyError> {
    object.get(key).map_or_else(
        || Ok(BTreeMap::new()),
        |value| {
            serde_json::from_value(value.clone())
                .map_err(|_| ToolHostDependencyError::InvalidRequest)
        },
    )
}

fn connection_key(session: &str, workspace: &Path) -> String {
    format!("{session}\0{}", workspace.to_string_lossy())
}

fn map_event(event: ToolHostEvent) -> Result<DependencyToolEvent, ToolHostDependencyError> {
    Ok(match event {
        ToolHostEvent::Started { call_id } => DependencyToolEvent::Started { call_id },
        ToolHostEvent::Progress {
            call_id,
            message,
            completed,
            total,
        } => DependencyToolEvent::Progress {
            call_id,
            message,
            completed,
            total,
        },
        ToolHostEvent::Output {
            call_id,
            stream,
            content,
        } => DependencyToolEvent::Output {
            call_id,
            stream: match stream {
                OutputStream::Standard => DependencyOutputStream::Standard,
                OutputStream::Error => DependencyOutputStream::Error,
            },
            content,
        },
        ToolHostEvent::Completed {
            call_id,
            result,
            artifact,
            truncated,
        } => DependencyToolEvent::Completed {
            call_id,
            result,
            artifact: artifact.map(|value| value.to_string()),
            truncated,
        },
        ToolHostEvent::Failed {
            call_id,
            code,
            message,
            retryable,
        } => DependencyToolEvent::Failed {
            call_id,
            code,
            message,
            retryable,
        },
        ToolHostEvent::Cancelled { call_id } => DependencyToolEvent::Cancelled { call_id },
        ToolHostEvent::Groups { .. } | ToolHostEvent::Tools { .. } => {
            return Err(ToolHostDependencyError::Protocol);
        }
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
