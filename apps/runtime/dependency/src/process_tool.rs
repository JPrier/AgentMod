//! Supervised process capability-host adapter.
#![allow(
    missing_docs,
    reason = "dependency-local process transport records are self-describing"
)]

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    process::Stdio,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{CancellationId, ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use agentmod_tool_protocol::{OutputStream, ToolHostCommand, ToolHostEvent};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::timeout,
};

use crate::tool::{
    DependencyOutputStream, DependencyToolCommand, DependencyToolEvent, ToolHostDependencyError,
    ToolHostDependencyPort,
};

#[derive(Clone, Debug)]
pub struct ProcessCapabilityDependencyConfig {
    pub program: String,
    pub arguments: Vec<String>,
    pub owner: String,
    pub allowed_executables: Vec<String>,
    pub maximum_frame_bytes: usize,
    pub request_timeout: Duration,
    pub authorization_key: [u8; 32],
}

struct Connection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
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
            || config.maximum_frame_bytes == 0
            || config.request_timeout.is_zero()
            || config.authorization_key == [0; 32]
        {
            return Err(ToolHostDependencyError::InvalidConfiguration);
        }
        Ok(Self {
            config: Arc::new(config),
            connections: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn connect(
        &self,
        session: &str,
        workspace: &Path,
    ) -> Result<Connection, ToolHostDependencyError> {
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
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
        let stdin = child
            .stdin
            .take()
            .ok_or(ToolHostDependencyError::Unavailable)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ToolHostDependencyError::Unavailable)?;
        Ok(Connection {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    async fn execute_once(
        &self,
        command: DependencyToolCommand,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError> {
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
                self.connect(&command.session_id, &command.workspace)?,
            );
        }
        let connection = connections
            .get_mut(&key)
            .ok_or(ToolHostDependencyError::Unavailable)?;
        let mut bytes = serde_json::to_vec(&wire).map_err(|_| ToolHostDependencyError::Protocol)?;
        bytes.push(b'\n');
        connection
            .stdin
            .write_all(&bytes)
            .await
            .map_err(|_| ToolHostDependencyError::Transport)?;
        connection
            .stdin
            .flush()
            .await
            .map_err(|_| ToolHostDependencyError::Transport)?;
        let mut events = Vec::new();
        loop {
            let frame = read_frame(&mut connection.stdout, self.config.maximum_frame_bytes).await?;
            let event: ToolHostEvent =
                serde_json::from_slice(&frame).map_err(|_| ToolHostDependencyError::Protocol)?;
            let terminal = matches!(
                event,
                ToolHostEvent::Completed { .. }
                    | ToolHostEvent::Failed { .. }
                    | ToolHostEvent::Cancelled { .. }
            );
            events.push(map_event(event)?);
            if terminal {
                return Ok(events);
            }
        }
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
        if result.is_err()
            && let Some(mut connection) = self.connections.lock().await.remove(&key)
        {
            let _ = connection.child.start_kill();
            let _ = connection.child.wait().await;
        }
        result
    }

    async fn shutdown(&self) {
        let connections = std::mem::take(&mut *self.connections.lock().await);
        for (_, mut connection) in connections {
            let _ = connection.child.start_kill();
            let _ = connection.child.wait().await;
        }
    }
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

async fn read_frame(
    reader: &mut BufReader<ChildStdout>,
    maximum: usize,
) -> Result<Vec<u8>, ToolHostDependencyError> {
    let mut frame = Vec::new();
    loop {
        let byte = reader
            .read_u8()
            .await
            .map_err(|_| ToolHostDependencyError::Transport)?;
        if byte == b'\n' {
            return Ok(frame);
        }
        if frame.len() >= maximum {
            return Err(ToolHostDependencyError::FrameTooLarge);
        }
        frame.push(byte);
    }
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
