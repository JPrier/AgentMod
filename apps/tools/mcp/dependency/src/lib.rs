//! MCP transport, lifecycle, capability, and protocol adapters.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationKey, ExpectedAuthorization, verify_authorization,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const MCP_VERSION: &str = "2025-06-18";
const HTTP_RESUME_ATTEMPTS: usize = 3;

/// Configured MCP transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyTransportConfig {
    /// Child process speaking MCP over stdio.
    Stdio {
        /// Executable without shell interpretation.
        program: String,
        /// Exact argument vector.
        arguments: Vec<String>,
        /// Explicit non-secret environment.
        environment: BTreeMap<String, String>,
    },
    /// Streamable HTTP endpoint.
    StreamableHttp {
        /// Absolute HTTP(S) endpoint.
        url: String,
        /// Optional environment variable containing a bearer token.
        bearer_token_environment: Option<String>,
    },
    /// Deterministic fixture used by network-free tests.
    Mock {
        /// Advertised tools.
        tools: Vec<DependencyTool>,
        /// Advertised resources.
        resources: Vec<DependencyResource>,
        /// Advertised prompts.
        prompts: Vec<DependencyPrompt>,
        /// Fixed call results keyed by `kind:name`.
        results: BTreeMap<String, Value>,
    },
}

/// Server configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyServerConfig {
    /// Stable server namespace.
    pub id: String,
    /// Human-readable name.
    pub display_name: String,
    /// Whether the server is active for this host instance.
    pub active: bool,
    /// Transport.
    pub transport: DependencyTransportConfig,
}

/// Dependency bounds and server catalog.
#[derive(Clone, Debug)]
pub struct McpDependencyConfig {
    /// Configured servers.
    pub servers: Vec<DependencyServerConfig>,
    /// Client identity sent during initialization.
    pub client_name: String,
    /// Client version.
    pub client_version: String,
    /// Request deadline.
    pub request_timeout: Duration,
    /// Maximum JSON response bytes.
    pub maximum_message_bytes: usize,
    /// Maximum registered servers.
    pub maximum_servers: usize,
    /// Runtime owner identity.
    pub authorization_owner: String,
    /// Runtime session identity.
    pub authorization_session: String,
    /// Hex-encoded local authorization key.
    pub authorization_key_hex: String,
    /// Durable single-use nonce records.
    pub authorization_replay_root: PathBuf,
    /// Durable Streamable HTTP session, cursor, and pending-request records.
    pub http_state_root: PathBuf,
}

/// Dependency-owned exact action authorization.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyAuthorization {
    /// Protocol call ID.
    pub call_id: String,
    /// Exact tool/action ID.
    pub action: String,
    /// Runtime-supplied digest.
    pub normalized_digest: String,
    /// Signed grant.
    pub grant: String,
    /// Original service arguments used for independent reconstruction.
    pub arguments: Value,
    /// Cross-process cancellation ID bound into the canonical operation.
    pub cancellation_id: String,
}

/// Tool descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct DependencyTool {
    /// Unqualified MCP tool name.
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: String,
    /// JSON Schema.
    #[serde(default, rename = "inputSchema")]
    pub input_schema: Value,
}

/// Resource descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct DependencyResource {
    /// Resource URI.
    pub uri: String,
    /// Display name.
    #[serde(default)]
    pub name: String,
    /// MIME type.
    #[serde(default, rename = "mimeType")]
    pub mime_type: Option<String>,
}

/// Prompt descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct DependencyPrompt {
    /// Prompt name.
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: String,
}

/// Normalized server capability snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCapabilities {
    /// Server namespace.
    pub server_id: String,
    /// Negotiated protocol version.
    pub protocol_version: String,
    /// Tools.
    pub tools: Vec<DependencyTool>,
    /// Resources.
    pub resources: Vec<DependencyResource>,
    /// Prompts.
    pub prompts: Vec<DependencyPrompt>,
}

/// Invocation kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyInvocationKind {
    /// Tool call.
    Tool,
    /// Resource read.
    Resource,
    /// Prompt expansion.
    Prompt,
}

/// Invocation request.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyInvokeRequest {
    /// Exact authorization envelope.
    pub authorization: DependencyAuthorization,
    /// Server namespace.
    pub server_id: String,
    /// Operation kind.
    pub kind: DependencyInvocationKind,
    /// Tool/prompt name or resource URI.
    pub name: String,
    /// Structured arguments.
    pub arguments: Value,
    /// Opaque cancellation ID.
    pub cancellation_id: String,
}

/// Progress notification.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyProgress {
    /// Optional progress token.
    pub token: Option<Value>,
    /// Raw progress payload.
    pub value: Value,
}

/// Normalized invocation response.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyInvokeResponse {
    /// Raw provider-neutral MCP result.
    pub result: Value,
    /// Progress observed before completion.
    pub progress: Vec<DependencyProgress>,
}

/// Server health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyServerHealth {
    /// Server namespace.
    pub server_id: String,
    /// Active in configuration.
    pub active: bool,
    /// Initialized successfully in this process.
    pub initialized: bool,
    /// Transport class.
    pub transport: String,
}

/// Dependency interface consumed by data.
#[async_trait]
pub trait McpDependencyPort: Send + Sync {
    /// Lists configured health without forcing startup.
    async fn list_servers(
        &self,
        authorization: DependencyAuthorization,
    ) -> Result<Vec<DependencyServerHealth>, McpDependencyError>;
    /// Initializes or refreshes one server and lists capabilities.
    async fn capabilities(
        &self,
        server_id: &str,
        authorization: DependencyAuthorization,
    ) -> Result<DependencyCapabilities, McpDependencyError>;
    /// Invokes an MCP operation.
    async fn invoke(
        &self,
        request: DependencyInvokeRequest,
    ) -> Result<DependencyInvokeResponse, McpDependencyError>;
    /// Cancels an active request.
    async fn cancel(&self, cancellation_id: &str) -> Result<(), McpDependencyError>;
    /// Shuts all child transports down.
    async fn shutdown(&self);
}

struct StdioConnection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PendingHttpRequest {
    request_id: String,
    operation_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableHttpState {
    schema_version: u32,
    server_id: String,
    server_identity: String,
    owner: String,
    authorization_session: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    protocol_version: Option<String>,
    session_id: Option<String>,
    last_event_id: Option<String>,
    pending_request: Option<PendingHttpRequest>,
}

#[derive(Deserialize, Serialize)]
struct StoredHttpState {
    checksum: String,
    state: DurableHttpState,
}

#[derive(Default)]
struct RuntimeState {
    initialized: bool,
    protocol_version: Option<String>,
    session_id: Option<String>,
    last_event_id: Option<String>,
    pending_request: Option<PendingHttpRequest>,
    stdio: Option<StdioConnection>,
}

struct Server {
    config: DependencyServerConfig,
    http_state_path: Option<PathBuf>,
    identity: String,
    http_request_lock: Mutex<()>,
    state: Mutex<RuntimeState>,
}

fn server_identity(
    server: &DependencyServerConfig,
    owner: &str,
    authorization_session: &str,
) -> Result<String, McpDependencyError> {
    let transport = match &server.transport {
        DependencyTransportConfig::StreamableHttp {
            url,
            bearer_token_environment,
        } => json!({
            "transport": "streamable_http",
            "url": url,
            "bearer_token_environment": bearer_token_environment,
        }),
        DependencyTransportConfig::Stdio { .. } => json!({"transport":"stdio"}),
        DependencyTransportConfig::Mock { .. } => json!({"transport":"mock"}),
    };
    serde_json::to_vec(&json!({
        "server_id": server.id,
        "owner": owner,
        "authorization_session": authorization_session,
        "transport": transport,
    }))
    .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
    .map_err(|_| McpDependencyError::InvalidConfiguration)
}

fn load_http_state(
    path: &Path,
    server: &DependencyServerConfig,
    identity: &str,
    owner: &str,
    authorization_session: &str,
) -> Result<Option<DurableHttpState>, McpDependencyError> {
    let backup = path.with_extension("backup");
    let primary = read_http_state(path);
    let fallback = read_http_state(&backup);
    let selected = match (primary, fallback) {
        (Ok(Some(state)), _) => {
            if backup.exists() {
                fs::remove_file(&backup).map_err(|_| McpDependencyError::HttpState)?;
            }
            Some(state)
        }
        (Ok(None) | Err(_), Ok(Some(state))) => {
            if path.exists() {
                fs::remove_file(path).map_err(|_| McpDependencyError::HttpState)?;
            }
            fs::rename(&backup, path).map_err(|_| McpDependencyError::HttpState)?;
            Some(state)
        }
        (Ok(None), Ok(None)) => None,
        (Ok(None), Err(_)) | (Err(_), Ok(None) | Err(_)) => {
            return Err(McpDependencyError::HttpState);
        }
    };
    let Some(state) = selected else {
        return Ok(None);
    };
    if state.schema_version != 1
        || state.server_id != server.id
        || state.server_identity != identity
        || state.owner != owner
        || state.authorization_session != authorization_session
        || state
            .protocol_version
            .as_deref()
            .is_some_and(|value| value != MCP_VERSION)
        || state
            .session_id
            .as_deref()
            .is_some_and(|value| !valid_http_identifier(value))
        || state
            .last_event_id
            .as_deref()
            .is_some_and(|value| !valid_http_identifier(value))
        || state.pending_request.as_ref().is_some_and(|pending| {
            !valid_http_identifier(&pending.request_id)
                || pending.operation_hash.len() != 64
                || !pending
                    .operation_hash
                    .bytes()
                    .all(|value| value.is_ascii_hexdigit())
                || state.session_id.is_none()
                || state.last_event_id.is_none()
        })
    {
        return Err(McpDependencyError::HttpState);
    }
    Ok(Some(state))
}

fn read_http_state(path: &Path) -> Result<Option<DurableHttpState>, McpDependencyError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|_| McpDependencyError::HttpState)?;
    let stored: StoredHttpState =
        serde_json::from_slice(&bytes).map_err(|_| McpDependencyError::HttpState)?;
    let checksum = serde_json::to_vec(&stored.state)
        .map(|value| blake3::hash(&value).to_hex().to_string())
        .map_err(|_| McpDependencyError::HttpState)?;
    if checksum != stored.checksum {
        return Err(McpDependencyError::HttpState);
    }
    Ok(Some(stored.state))
}

fn write_http_state(path: &Path, state: &DurableHttpState) -> Result<(), McpDependencyError> {
    let bytes = serde_json::to_vec(state).map_err(|_| McpDependencyError::HttpState)?;
    let stored = StoredHttpState {
        checksum: blake3::hash(&bytes).to_hex().to_string(),
        state: state.clone(),
    };
    let temporary = path.with_extension(format!("{}.next", uuid::Uuid::now_v7()));
    let backup = path.with_extension("backup");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|_| McpDependencyError::HttpState)?;
    serde_json::to_writer(&mut file, &stored).map_err(|_| McpDependencyError::HttpState)?;
    file.write_all(b"\n")
        .and_then(|()| file.sync_all())
        .map_err(|_| McpDependencyError::HttpState)?;
    if backup.exists() {
        fs::remove_file(&backup).map_err(|_| McpDependencyError::HttpState)?;
    }
    if path.exists() {
        fs::rename(path, &backup).map_err(|_| McpDependencyError::HttpState)?;
    }
    if fs::rename(&temporary, path).is_err() {
        if backup.exists() {
            let _ = fs::rename(&backup, path);
        }
        let _ = fs::remove_file(&temporary);
        return Err(McpDependencyError::HttpState);
    }
    if backup.exists() {
        fs::remove_file(backup).map_err(|_| McpDependencyError::HttpState)?;
    }
    Ok(())
}

fn valid_http_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && !value.chars().any(char::is_control)
        && !value.contains(['\r', '\n'])
}

/// First-party MCP dependency.
#[derive(Clone)]
pub struct McpDependency {
    config: Arc<McpDependencyConfig>,
    servers: Arc<BTreeMap<String, Arc<Server>>>,
    client: reqwest::Client,
    active: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    authorization_key: Arc<AuthorizationKey>,
}

impl McpDependency {
    /// Validates and constructs an MCP adapter.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds, duplicate IDs, unsafe stdio configuration, and invalid URLs.
    pub fn new(mut config: McpDependencyConfig) -> Result<Self, McpDependencyError> {
        if config.client_name.trim().is_empty()
            || config.client_version.trim().is_empty()
            || config.request_timeout.is_zero()
            || config.maximum_message_bytes == 0
            || config.maximum_servers == 0
            || config.servers.len() > config.maximum_servers
            || config.authorization_owner.trim().is_empty()
            || config.authorization_session.trim().is_empty()
            || config.authorization_replay_root.as_os_str().is_empty()
            || config.http_state_root.as_os_str().is_empty()
        {
            return Err(McpDependencyError::InvalidConfiguration);
        }
        let authorization_key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| McpDependencyError::InvalidConfiguration)?;
        config.authorization_key_hex.clear();
        let mut server_ids = BTreeSet::new();
        for server in &config.servers {
            validate_server(server)?;
            if !server_ids.insert(server.id.clone()) {
                return Err(McpDependencyError::InvalidConfiguration);
            }
        }
        std::fs::create_dir_all(&config.authorization_replay_root)
            .map_err(|_| McpDependencyError::ReplayState)?;
        fs::create_dir_all(&config.http_state_root).map_err(|_| McpDependencyError::HttpState)?;
        let mut servers = BTreeMap::new();
        for server in &config.servers {
            let identity = server_identity(
                server,
                &config.authorization_owner,
                &config.authorization_session,
            )?;
            let http_state_path = matches!(
                server.transport,
                DependencyTransportConfig::StreamableHttp { .. }
            )
            .then(|| config.http_state_root.join(format!("{}.json", server.id)));
            let durable = http_state_path
                .as_deref()
                .map(|path| {
                    load_http_state(
                        path,
                        server,
                        &identity,
                        &config.authorization_owner,
                        &config.authorization_session,
                    )
                })
                .transpose()?
                .flatten();
            servers.insert(
                server.id.clone(),
                Arc::new(Server {
                    config: server.clone(),
                    http_state_path,
                    identity,
                    http_request_lock: Mutex::new(()),
                    state: Mutex::new(RuntimeState {
                        protocol_version: durable
                            .as_ref()
                            .and_then(|state| state.protocol_version.clone()),
                        session_id: durable.as_ref().and_then(|state| state.session_id.clone()),
                        last_event_id: durable
                            .as_ref()
                            .and_then(|state| state.last_event_id.clone()),
                        pending_request: durable.and_then(|state| state.pending_request),
                        ..RuntimeState::default()
                    }),
                }),
            );
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| McpDependencyError::InvalidConfiguration)?;
        Ok(Self {
            config: Arc::new(config),
            servers: Arc::new(servers),
            client,
            active: Arc::new(Mutex::new(BTreeMap::new())),
            authorization_key: Arc::new(authorization_key),
        })
    }

    fn authorize(&self, authorization: &DependencyAuthorization) -> Result<(), McpDependencyError> {
        let canonical = canonical_operation(
            &authorization.action,
            &authorization.arguments,
            &authorization.cancellation_id,
        )?;
        let digest = ContentHash::digest(&canonical);
        if digest.to_hex() != authorization.normalized_digest {
            return Err(McpDependencyError::Authorization);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| McpDependencyError::Authorization)?
            .as_millis();
        let claims = verify_authorization(
            &authorization.grant,
            &self.authorization_key,
            ExpectedAuthorization {
                owner: &self.config.authorization_owner,
                session: &self.config.authorization_session,
                call_id: &authorization.call_id,
                action: &authorization.action,
                normalized_digest: digest,
            },
            TimestampMillis::new(
                i64::try_from(now).map_err(|_| McpDependencyError::Authorization)?,
            ),
        )
        .map_err(|_| McpDependencyError::Authorization)?;
        let nonce_hash = blake3::hash(claims.nonce.as_bytes()).to_hex();
        let path = self
            .config
            .authorization_replay_root
            .join(format!("{nonce_hash}.used"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    McpDependencyError::AuthorizationReplay
                } else {
                    McpDependencyError::ReplayState
                }
            })?;
        file.write_all(b"used\n")
            .and_then(|()| file.sync_all())
            .map_err(|_| McpDependencyError::ReplayState)
    }

    fn server(&self, id: &str) -> Result<Arc<Server>, McpDependencyError> {
        self.servers
            .get(id)
            .filter(|server| server.config.active)
            .cloned()
            .ok_or(McpDependencyError::ServerUnavailable)
    }

    async fn request(
        &self,
        server: &Server,
        method: &str,
        params: Value,
        cancellation_id: Option<&str>,
    ) -> Result<(Value, Vec<DependencyProgress>), McpDependencyError> {
        let token = CancellationToken::new();
        if let Some(id) = cancellation_id {
            let mut active = self.active.lock().await;
            if active.insert(id.to_owned(), token.clone()).is_some() {
                return Err(McpDependencyError::DuplicateCancellation);
            }
        }
        let request_id = uuid::Uuid::now_v7().to_string();
        let request = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        });
        let operation = async {
            match &server.config.transport {
                DependencyTransportConfig::Mock { results, .. } => results
                    .get(&format!(
                        "{method}:{}",
                        request["params"]["name"].as_str().unwrap_or_default()
                    ))
                    .or_else(|| results.get(method))
                    .cloned()
                    .map(|value| (value, Vec::new()))
                    .ok_or(McpDependencyError::RemoteError),
                DependencyTransportConfig::Stdio { .. } => {
                    self.stdio_request(server, &request_id, request).await
                }
                DependencyTransportConfig::StreamableHttp { .. } => {
                    self.http_request(server, request).await
                }
            }
        };
        let result = tokio::select! {
            () = token.cancelled() => Err(McpDependencyError::Cancelled),
            value = timeout(self.config.request_timeout, operation) => {
                value.map_err(|_| McpDependencyError::Timeout)?
            }
        };
        if let Some(id) = cancellation_id {
            self.active.lock().await.remove(id);
        }
        result
    }

    async fn ensure_initialized(&self, server: &Server) -> Result<String, McpDependencyError> {
        if matches!(
            server.config.transport,
            DependencyTransportConfig::Mock { .. }
        ) {
            server.state.lock().await.initialized = true;
            return Ok(MCP_VERSION.to_owned());
        }
        if server.state.lock().await.initialized {
            return Ok(MCP_VERSION.to_owned());
        }
        let (result, _) = self
            .request(
                server,
                "initialize",
                json!({
                    "protocolVersion": MCP_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": self.config.client_name,
                        "version": self.config.client_version,
                    }
                }),
                None,
            )
            .await?;
        let version = result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .unwrap_or(MCP_VERSION)
            .to_owned();
        if let DependencyTransportConfig::Stdio { .. } = server.config.transport {
            let mut state = server.state.lock().await;
            let connection = state
                .stdio
                .as_mut()
                .ok_or(McpDependencyError::ServerUnavailable)?;
            write_stdio_message(
                &mut connection.stdin,
                &json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}),
            )
            .await?;
            state.initialized = true;
        } else {
            let mut state = server.state.lock().await;
            state.initialized = true;
            state.protocol_version = Some(version.clone());
            self.persist_http_runtime_state(server, &state)?;
        }
        Ok(version)
    }

    async fn stdio_request(
        &self,
        server: &Server,
        request_id: &str,
        request: Value,
    ) -> Result<(Value, Vec<DependencyProgress>), McpDependencyError> {
        let mut state = server.state.lock().await;
        if state.stdio.is_none() {
            state.stdio = Some(spawn_stdio(&server.config)?);
        }
        let connection = state
            .stdio
            .as_mut()
            .ok_or(McpDependencyError::ServerUnavailable)?;
        write_stdio_message(&mut connection.stdin, &request).await?;
        let mut progress = Vec::new();
        loop {
            let message =
                read_stdio_message(&mut connection.stdout, self.config.maximum_message_bytes)
                    .await?;
            if message.get("id").and_then(Value::as_str) == Some(request_id) {
                if let Some(error) = message.get("error") {
                    let _ = error;
                    return Err(McpDependencyError::RemoteError);
                }
                return message
                    .get("result")
                    .cloned()
                    .map(|result| (result, progress))
                    .ok_or(McpDependencyError::Protocol);
            }
            if message.get("method").and_then(Value::as_str) == Some("notifications/progress") {
                let params = message.get("params").cloned().unwrap_or(Value::Null);
                progress.push(DependencyProgress {
                    token: params.get("progressToken").cloned(),
                    value: params,
                });
            }
        }
    }

    fn persist_http_runtime_state(
        &self,
        server: &Server,
        state: &RuntimeState,
    ) -> Result<(), McpDependencyError> {
        let Some(path) = &server.http_state_path else {
            return Ok(());
        };
        write_http_state(
            path,
            &DurableHttpState {
                schema_version: 1,
                server_id: server.config.id.clone(),
                server_identity: server.identity.clone(),
                owner: self.config.authorization_owner.clone(),
                authorization_session: self.config.authorization_session.clone(),
                protocol_version: state.protocol_version.clone(),
                session_id: state.session_id.clone(),
                last_event_id: state.last_event_id.clone(),
                pending_request: state.pending_request.clone(),
            },
        )
    }

    async fn update_http_session(
        &self,
        server: &Server,
        response: &reqwest::Response,
    ) -> Result<(), McpDependencyError> {
        let Some(session) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
        else {
            return Ok(());
        };
        if !valid_http_identifier(session) {
            return Err(McpDependencyError::Protocol);
        }
        let mut state = server.state.lock().await;
        if state.session_id.as_deref() != Some(session) {
            state.last_event_id = None;
            state.pending_request = None;
        }
        state.session_id = Some(session.to_owned());
        self.persist_http_runtime_state(server, &state)
    }

    async fn update_http_cursor(
        &self,
        server: &Server,
        last_event_id: Option<String>,
        pending_request: Option<PendingHttpRequest>,
    ) -> Result<(), McpDependencyError> {
        if last_event_id
            .as_deref()
            .is_some_and(|value| !valid_http_identifier(value))
        {
            return Err(McpDependencyError::Protocol);
        }
        let mut state = server.state.lock().await;
        if let Some(last_event_id) = last_event_id {
            state.last_event_id = Some(last_event_id);
        }
        state.pending_request = pending_request;
        self.persist_http_runtime_state(server, &state)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the transport transaction keeps durable pending-state writes adjacent to each network boundary"
    )]
    async fn http_request(
        &self,
        server: &Server,
        request: Value,
    ) -> Result<(Value, Vec<DependencyProgress>), McpDependencyError> {
        let _request_guard = server.http_request_lock.lock().await;
        let DependencyTransportConfig::StreamableHttp {
            url,
            bearer_token_environment,
        } = &server.config.transport
        else {
            return Err(McpDependencyError::Protocol);
        };
        let request_id = request
            .get("id")
            .and_then(Value::as_str)
            .ok_or(McpDependencyError::Protocol)?;
        let operation_hash = http_operation_hash(&request)?;
        let persisted = {
            let state = server.state.lock().await;
            state.pending_request.clone().map(|pending| {
                (
                    pending,
                    state.session_id.clone(),
                    state.last_event_id.clone(),
                )
            })
        };
        if let Some((pending, Some(session_id), Some(last_event_id))) = persisted {
            if pending.operation_hash != operation_hash {
                return Err(McpDependencyError::CursorConflict);
            }
            return self
                .resume_http_request(
                    server,
                    url,
                    bearer_token_environment.as_deref(),
                    &pending.request_id,
                    &operation_hash,
                    session_id,
                    last_event_id,
                    Vec::new(),
                )
                .await;
        }
        let mut builder = self
            .client
            .post(url)
            .header("accept", "application/json, text/event-stream")
            .json(&request);
        let (session_id, protocol_version) = {
            let state = server.state.lock().await;
            (state.session_id.clone(), state.protocol_version.clone())
        };
        if let Some(session_id) = session_id {
            builder = builder.header("mcp-session-id", session_id);
        }
        if let Some(protocol_version) = protocol_version {
            builder = builder.header("mcp-protocol-version", protocol_version);
        }
        if let Some(variable) = bearer_token_environment {
            let token =
                std::env::var(variable).map_err(|_| McpDependencyError::SecretUnavailable)?;
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| McpDependencyError::Transport)?;
        self.update_http_session(server, &response).await?;
        if !response.status().is_success() {
            return Err(McpDependencyError::RemoteError);
        }
        let previous_event_id = server.state.lock().await.last_event_id.clone();
        let parsed = read_http_response(
            response,
            request_id,
            self.config.maximum_message_bytes,
            previous_event_id.as_deref(),
        )
        .await?;
        if let Some(result) = parsed.result {
            self.update_http_cursor(server, parsed.last_event_id, None)
                .await?;
            return Ok((result, parsed.progress));
        }
        let last_event_id = parsed
            .last_event_id
            .or(previous_event_id)
            .ok_or(McpDependencyError::Protocol)?;
        let session_id = server
            .state
            .lock()
            .await
            .session_id
            .clone()
            .ok_or(McpDependencyError::Protocol)?;
        self.update_http_cursor(
            server,
            Some(last_event_id.clone()),
            Some(PendingHttpRequest {
                request_id: request_id.to_owned(),
                operation_hash: operation_hash.clone(),
            }),
        )
        .await?;
        self.resume_http_request(
            server,
            url,
            bearer_token_environment.as_deref(),
            request_id,
            &operation_hash,
            session_id,
            last_event_id,
            parsed.progress,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the dependency binds every durable HTTP resumption identity explicitly"
    )]
    async fn resume_http_request(
        &self,
        server: &Server,
        url: &str,
        bearer_token_environment: Option<&str>,
        request_id: &str,
        operation_hash: &str,
        session_id: String,
        mut last_event_id: String,
        mut progress: Vec<DependencyProgress>,
    ) -> Result<(Value, Vec<DependencyProgress>), McpDependencyError> {
        for _ in 0..HTTP_RESUME_ATTEMPTS {
            let mut resume = self
                .client
                .get(url)
                .header("accept", "text/event-stream")
                .header("last-event-id", &last_event_id)
                .header("mcp-session-id", &session_id);
            if let Some(protocol_version) = server.state.lock().await.protocol_version.clone() {
                resume = resume.header("mcp-protocol-version", protocol_version);
            }
            if let Some(variable) = bearer_token_environment {
                let token =
                    std::env::var(variable).map_err(|_| McpDependencyError::SecretUnavailable)?;
                resume = resume.bearer_auth(token);
            }
            let response = resume
                .send()
                .await
                .map_err(|_| McpDependencyError::Transport)?;
            self.update_http_session(server, &response).await?;
            if server.state.lock().await.session_id.as_deref() != Some(session_id.as_str()) {
                return Err(McpDependencyError::Protocol);
            }
            if !response.status().is_success() {
                return Err(McpDependencyError::RemoteError);
            }
            let resumed = read_http_response(
                response,
                request_id,
                self.config.maximum_message_bytes,
                Some(&last_event_id),
            )
            .await?;
            progress.extend(resumed.progress);
            if let Some(event_id) = resumed.last_event_id {
                last_event_id.clone_from(&event_id);
            }
            if let Some(result) = resumed.result {
                self.update_http_cursor(server, Some(last_event_id), None)
                    .await?;
                return Ok((result, progress));
            }
            self.update_http_cursor(
                server,
                Some(last_event_id.clone()),
                Some(PendingHttpRequest {
                    request_id: request_id.to_owned(),
                    operation_hash: operation_hash.to_owned(),
                }),
            )
            .await?;
        }
        Err(McpDependencyError::ResumeExhausted)
    }
}

struct ParsedHttpResponse {
    result: Option<Value>,
    progress: Vec<DependencyProgress>,
    last_event_id: Option<String>,
}

async fn read_http_response(
    response: reqwest::Response,
    request_id: &str,
    maximum_message_bytes: usize,
    previous_event_id: Option<&str>,
) -> Result<ParsedHttpResponse, McpDependencyError> {
    let is_sse = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"));
    let bytes = response
        .bytes()
        .await
        .map_err(|_| McpDependencyError::Transport)?;
    if bytes.len() > maximum_message_bytes {
        return Err(McpDependencyError::MessageTooLarge);
    }
    if is_sse || bytes.starts_with(b"data:") || bytes.starts_with(b"id:") {
        parse_sse_response(&bytes, request_id, previous_event_id)
    } else {
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| McpDependencyError::Protocol)?;
        let (result, progress) = response_result(&value)?;
        Ok(ParsedHttpResponse {
            result: Some(result),
            progress,
            last_event_id: None,
        })
    }
}

fn parse_sse_response(
    bytes: &[u8],
    request_id: &str,
    previous_event_id: Option<&str>,
) -> Result<ParsedHttpResponse, McpDependencyError> {
    let text = std::str::from_utf8(bytes).map_err(|_| McpDependencyError::Protocol)?;
    let mut result = None;
    let mut progress = Vec::new();
    let mut last_event_id = previous_event_id.map(str::to_owned);
    let mut event_id = None;
    let mut data = Vec::new();
    let mut saw_frame = false;
    for line in text.lines().chain(std::iter::once("")) {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            if data.is_empty() {
                event_id = None;
            } else {
                saw_frame = true;
                let value: Value = serde_json::from_str(&data.join("\n"))
                    .map_err(|_| McpDependencyError::Protocol)?;
                let id = event_id.take().filter(|id: &String| !id.is_empty());
                if id.as_deref() == last_event_id.as_deref() {
                    data.clear();
                    continue;
                }
                if let Some(id) = id {
                    last_event_id = Some(id);
                }
                if value.get("method").and_then(Value::as_str) == Some("notifications/progress") {
                    let params = value.get("params").cloned().unwrap_or(Value::Null);
                    progress.push(DependencyProgress {
                        token: params.get("progressToken").cloned(),
                        value: params,
                    });
                } else if value.get("id").and_then(Value::as_str) == Some(request_id) {
                    if value.get("error").is_some() {
                        return Err(McpDependencyError::RemoteError);
                    }
                    result = value.get("result").cloned();
                }
                data.clear();
            }
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("id:") {
            event_id = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start().to_owned());
        }
    }
    if result.is_none() && progress.is_empty() && !saw_frame {
        return Err(McpDependencyError::Protocol);
    }
    Ok(ParsedHttpResponse {
        result,
        progress,
        last_event_id,
    })
}

#[async_trait]
impl McpDependencyPort for McpDependency {
    async fn list_servers(
        &self,
        authorization: DependencyAuthorization,
    ) -> Result<Vec<DependencyServerHealth>, McpDependencyError> {
        if authorization.action != "mcp.server.list" || authorization.arguments != json!({}) {
            return Err(McpDependencyError::Authorization);
        }
        self.authorize(&authorization)?;
        let mut result = Vec::with_capacity(self.servers.len());
        for server in self.servers.values() {
            let state = server.state.lock().await;
            result.push(DependencyServerHealth {
                server_id: server.config.id.clone(),
                active: server.config.active,
                initialized: state.initialized,
                transport: match server.config.transport {
                    DependencyTransportConfig::Stdio { .. } => "stdio",
                    DependencyTransportConfig::StreamableHttp { .. } => "streamable_http",
                    DependencyTransportConfig::Mock { .. } => "mock",
                }
                .to_owned(),
            });
        }
        Ok(result)
    }

    async fn capabilities(
        &self,
        server_id: &str,
        authorization: DependencyAuthorization,
    ) -> Result<DependencyCapabilities, McpDependencyError> {
        if authorization.action != "mcp.capabilities"
            || authorization.arguments != json!({"server_id":server_id})
        {
            return Err(McpDependencyError::Authorization);
        }
        self.authorize(&authorization)?;
        let server = self.server(server_id)?;
        let protocol_version = self.ensure_initialized(&server).await?;
        let (tools, resources, prompts) = if let DependencyTransportConfig::Mock {
            tools,
            resources,
            prompts,
            ..
        } = &server.config.transport
        {
            (tools.clone(), resources.clone(), prompts.clone())
        } else {
            let tools = self
                .request(&server, "tools/list", json!({}), None)
                .await?
                .0
                .get("tools")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|_| McpDependencyError::Protocol)?
                .unwrap_or_default();
            let resources = self
                .request(&server, "resources/list", json!({}), None)
                .await
                .ok()
                .and_then(|value| value.0.get("resources").cloned())
                .map(serde_json::from_value)
                .transpose()
                .map_err(|_| McpDependencyError::Protocol)?
                .unwrap_or_default();
            let prompts = self
                .request(&server, "prompts/list", json!({}), None)
                .await
                .ok()
                .and_then(|value| value.0.get("prompts").cloned())
                .map(serde_json::from_value)
                .transpose()
                .map_err(|_| McpDependencyError::Protocol)?
                .unwrap_or_default();
            (tools, resources, prompts)
        };
        Ok(DependencyCapabilities {
            server_id: server_id.to_owned(),
            protocol_version,
            tools,
            resources,
            prompts,
        })
    }

    async fn invoke(
        &self,
        request: DependencyInvokeRequest,
    ) -> Result<DependencyInvokeResponse, McpDependencyError> {
        let kind = match request.kind {
            DependencyInvocationKind::Tool => "tool",
            DependencyInvocationKind::Resource => "resource",
            DependencyInvocationKind::Prompt => "prompt",
        };
        let expected_arguments = json!({
            "server_id":request.server_id,
            "kind":kind,
            "name":request.name,
            "arguments":request.arguments,
        });
        if request.authorization.action != "mcp.invoke"
            || request.authorization.arguments != expected_arguments
        {
            return Err(McpDependencyError::Authorization);
        }
        self.authorize(&request.authorization)?;
        if request.name.trim().is_empty() || request.cancellation_id.trim().is_empty() {
            return Err(McpDependencyError::InvalidRequest);
        }
        let server = self.server(&request.server_id)?;
        self.ensure_initialized(&server).await?;
        let (method, params) = match request.kind {
            DependencyInvocationKind::Tool => (
                "tools/call",
                json!({"name":request.name,"arguments":request.arguments}),
            ),
            DependencyInvocationKind::Resource => (
                "resources/read",
                json!({"name":request.name,"uri":request.name}),
            ),
            DependencyInvocationKind::Prompt => (
                "prompts/get",
                json!({"name":request.name,"arguments":request.arguments}),
            ),
        };
        let (result, progress) = self
            .request(&server, method, params, Some(&request.cancellation_id))
            .await?;
        Ok(DependencyInvokeResponse { result, progress })
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<(), McpDependencyError> {
        self.active
            .lock()
            .await
            .get(cancellation_id)
            .cloned()
            .ok_or(McpDependencyError::UnknownCancellation)?
            .cancel();
        Ok(())
    }

    async fn shutdown(&self) {
        for server in self.servers.values() {
            let mut state = server.state.lock().await;
            if let Some(mut connection) = state.stdio.take() {
                let _ = connection.child.start_kill();
                let _ = connection.child.wait().await;
            }
            state.initialized = false;
        }
    }
}

fn canonical_operation(
    action: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<Vec<u8>, McpDependencyError> {
    if action.trim().is_empty() || cancellation_id.trim().is_empty() || !arguments.is_object() {
        return Err(McpDependencyError::Authorization);
    }
    let normalized = normalize_json(arguments);
    serde_json::to_vec(&(action, cancellation_id, normalized))
        .map_err(|_| McpDependencyError::Authorization)
}

fn http_operation_hash(request: &Value) -> Result<String, McpDependencyError> {
    let mut operation = request.clone();
    operation
        .as_object_mut()
        .ok_or(McpDependencyError::Protocol)?
        .remove("id");
    serde_json::to_vec(&normalize_json(&operation))
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|_| McpDependencyError::Protocol)
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

fn validate_server(server: &DependencyServerConfig) -> Result<(), McpDependencyError> {
    if server.id.is_empty()
        || server.id.len() > 64
        || !server
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(McpDependencyError::InvalidConfiguration);
    }
    match &server.transport {
        DependencyTransportConfig::Stdio {
            program,
            arguments,
            environment,
        } => {
            if program.trim().is_empty()
                || program.contains('\0')
                || arguments.iter().any(|value| value.contains('\0'))
                || environment.iter().any(|(key, value)| {
                    key.is_empty()
                        || key.contains(['=', '\0'])
                        || value.contains('\0')
                        || sensitive_name(key)
                })
            {
                return Err(McpDependencyError::InvalidConfiguration);
            }
        }
        DependencyTransportConfig::StreamableHttp {
            url,
            bearer_token_environment,
        } => {
            let parsed =
                url::Url::parse(url).map_err(|_| McpDependencyError::InvalidConfiguration)?;
            let host = parsed
                .host_str()
                .ok_or(McpDependencyError::InvalidConfiguration)?;
            let secure_transport = parsed.scheme() == "https"
                || (parsed.scheme() == "http" && matches!(host, "localhost" | "127.0.0.1" | "::1"));
            if !secure_transport {
                return Err(McpDependencyError::InvalidConfiguration);
            }
            if bearer_token_environment.as_ref().is_some_and(|name| {
                name.is_empty()
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
            }) {
                return Err(McpDependencyError::InvalidConfiguration);
            }
        }
        DependencyTransportConfig::Mock { .. } => {}
    }
    Ok(())
}

fn sensitive_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    ["KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"]
        .iter()
        .any(|marker| upper.contains(marker))
}

fn spawn_stdio(config: &DependencyServerConfig) -> Result<StdioConnection, McpDependencyError> {
    let DependencyTransportConfig::Stdio {
        program,
        arguments,
        environment,
    } = &config.transport
    else {
        return Err(McpDependencyError::Protocol);
    };
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| McpDependencyError::Transport)?;
    let stdin = child.stdin.take().ok_or(McpDependencyError::Transport)?;
    let stdout = child.stdout.take().ok_or(McpDependencyError::Transport)?;
    Ok(StdioConnection {
        child,
        stdin,
        stdout: BufReader::new(stdout),
    })
}

async fn write_stdio_message(
    writer: &mut ChildStdin,
    value: &Value,
) -> Result<(), McpDependencyError> {
    let bytes = serde_json::to_vec(value).map_err(|_| McpDependencyError::Protocol)?;
    writer
        .write_all(format!("Content-Length: {}\r\n\r\n", bytes.len()).as_bytes())
        .await
        .map_err(|_| McpDependencyError::Transport)?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|_| McpDependencyError::Transport)?;
    writer
        .flush()
        .await
        .map_err(|_| McpDependencyError::Transport)
}

async fn read_stdio_message(
    reader: &mut BufReader<ChildStdout>,
    maximum: usize,
) -> Result<Value, McpDependencyError> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        if reader
            .read_line(&mut line)
            .await
            .map_err(|_| McpDependencyError::Transport)?
            == 0
        {
            return Err(McpDependencyError::Transport);
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some(value) = line.trim().strip_prefix("Content-Length:").map(str::trim) {
            content_length = value.parse::<usize>().ok();
        }
    }
    let length = content_length.ok_or(McpDependencyError::Protocol)?;
    if length == 0 || length > maximum {
        return Err(McpDependencyError::MessageTooLarge);
    }
    let mut bytes = vec![0; length];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|_| McpDependencyError::Transport)?;
    serde_json::from_slice(&bytes).map_err(|_| McpDependencyError::Protocol)
}

fn response_result(value: &Value) -> Result<(Value, Vec<DependencyProgress>), McpDependencyError> {
    if value.get("error").is_some() {
        return Err(McpDependencyError::RemoteError);
    }
    value
        .get("result")
        .cloned()
        .map(|result| (result, Vec::new()))
        .ok_or(McpDependencyError::Protocol)
}

/// Redacted dependency failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum McpDependencyError {
    /// Invalid bootstrap configuration.
    #[error("MCP dependency configuration is invalid")]
    InvalidConfiguration,
    /// Requested server is disabled or unknown.
    #[error("MCP server is unavailable")]
    ServerUnavailable,
    /// Invalid request.
    #[error("MCP request is invalid")]
    InvalidRequest,
    /// Authorization was absent, stale, mismatched, or invalid.
    #[error("MCP authorization denied")]
    Authorization,
    /// Authorization nonce was already consumed.
    #[error("MCP authorization replay")]
    AuthorizationReplay,
    /// Durable replay state could not be read or written safely.
    #[error("MCP authorization replay state unavailable")]
    ReplayState,
    /// Durable HTTP session or cursor state is corrupt or unavailable.
    #[error("MCP HTTP recovery state unavailable")]
    HttpState,
    /// Transport failed.
    #[error("MCP transport failed")]
    Transport,
    /// Protocol response was malformed.
    #[error("MCP protocol violation")]
    Protocol,
    /// Remote JSON-RPC error.
    #[error("MCP remote operation failed")]
    RemoteError,
    /// Request timed out.
    #[error("MCP request timed out")]
    Timeout,
    /// Resumable Streamable HTTP never produced the requested terminal result.
    #[error("MCP resumable HTTP attempts were exhausted")]
    ResumeExhausted,
    /// A different operation attempted to reuse a pending server cursor.
    #[error("MCP resumable HTTP cursor is bound to another operation")]
    CursorConflict,
    /// Request cancelled.
    #[error("MCP request was cancelled")]
    Cancelled,
    /// Duplicate cancellation ID.
    #[error("MCP cancellation identifier is already active")]
    DuplicateCancellation,
    /// Cancellation ID unknown.
    #[error("MCP cancellation identifier is unknown")]
    UnknownCancellation,
    /// Message exceeded bounds.
    #[error("MCP message exceeded the configured bound")]
    MessageTooLarge,
    /// Secret reference unavailable.
    #[error("MCP OAuth secret reference is unavailable")]
    SecretUnavailable,
}

#[cfg(test)]
mod tests {
    use agentmod_protocol_support::authorization::{AuthorizationClaims, seal_authorization};

    use super::*;

    const KEY: [u8; 32] = [7; 32];

    fn dependency(root: &std::path::Path) -> McpDependency {
        McpDependency::new(McpDependencyConfig {
            servers: vec![DependencyServerConfig {
                id: "docs".to_owned(),
                display_name: "Docs".to_owned(),
                active: true,
                transport: DependencyTransportConfig::Mock {
                    tools: vec![DependencyTool {
                        name: "lookup".to_owned(),
                        description: "Lookup docs".to_owned(),
                        input_schema: json!({"type":"object"}),
                    }],
                    resources: Vec::new(),
                    prompts: Vec::new(),
                    results: BTreeMap::from([(
                        "tools/call:lookup".to_owned(),
                        json!({"content":[{"type":"text","text":"result"}]}),
                    )]),
                },
            }],
            client_name: "agentmod".to_owned(),
            client_version: "0.1.0".to_owned(),
            request_timeout: Duration::from_secs(1),
            maximum_message_bytes: 1024 * 1024,
            maximum_servers: 8,
            authorization_owner: "owner".into(),
            authorization_session: "session".into(),
            authorization_key_hex: encode_hex(&KEY),
            authorization_replay_root: root.join("replay"),
            http_state_root: root.join("http-state"),
        })
        .expect("dependency")
    }

    fn authorization(
        action: &str,
        arguments: Value,
        cancellation_id: &str,
        call_id: &str,
        nonce: &str,
    ) -> DependencyAuthorization {
        let canonical =
            canonical_operation(action, &arguments, cancellation_id).expect("canonical");
        let digest = ContentHash::digest(&canonical);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: "owner".into(),
                session: "session".into(),
                call_id: call_id.into(),
                action: action.into(),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(i64::try_from(now).expect("time")),
                expires_at: TimestampMillis::new(i64::try_from(now + 30_000).expect("expiry")),
                nonce: nonce.into(),
            },
            &AuthorizationKey::from_bytes(KEY),
        )
        .expect("grant");
        DependencyAuthorization {
            call_id: call_id.into(),
            action: action.into(),
            normalized_digest: digest.to_hex(),
            grant,
            arguments,
            cancellation_id: cancellation_id.into(),
        }
    }

    fn encode_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").expect("write to string");
        }
        encoded
    }

    #[tokio::test]
    async fn mock_discovery_and_namespaced_call_are_deterministic() {
        let root = tempfile::tempdir().expect("root");
        let dependency = dependency(root.path());
        let capabilities = dependency
            .capabilities(
                "docs",
                authorization(
                    "mcp.capabilities",
                    json!({"server_id":"docs"}),
                    "cancel-capabilities",
                    "call-capabilities",
                    "nonce-capabilities",
                ),
            )
            .await
            .expect("capabilities");
        assert_eq!(capabilities.tools[0].name, "lookup");
        let response = dependency
            .invoke(DependencyInvokeRequest {
                authorization: authorization(
                    "mcp.invoke",
                    json!({
                        "server_id":"docs",
                        "kind":"tool",
                        "name":"lookup",
                        "arguments":{"query":"rust"}
                    }),
                    "cancel",
                    "call-invoke",
                    "nonce-invoke",
                ),
                server_id: "docs".to_owned(),
                kind: DependencyInvocationKind::Tool,
                name: "lookup".to_owned(),
                arguments: json!({"query":"rust"}),
                cancellation_id: "cancel".to_owned(),
            })
            .await
            .expect("call");
        assert_eq!(response.result["content"][0]["text"], "result");
    }

    #[tokio::test]
    async fn rejects_tampered_digest_before_operation() {
        let root = tempfile::tempdir().expect("root");
        let dependency = dependency(root.path());
        let mut authorization = authorization(
            "mcp.server.list",
            json!({}),
            "cancel-list",
            "call-list",
            "nonce-tampered",
        );
        authorization.normalized_digest = "00".repeat(32);

        let result = dependency.list_servers(authorization).await;

        assert_eq!(result, Err(McpDependencyError::Authorization));
    }

    #[tokio::test]
    async fn rejects_duplicate_nonce_in_the_same_process() {
        let root = tempfile::tempdir().expect("root");
        let dependency = dependency(root.path());
        let authorization = authorization(
            "mcp.server.list",
            json!({}),
            "cancel-list",
            "call-list",
            "nonce-duplicate",
        );
        dependency
            .list_servers(authorization.clone())
            .await
            .expect("first use");

        let replay = dependency.list_servers(authorization).await;

        assert_eq!(replay, Err(McpDependencyError::AuthorizationReplay));
    }

    #[tokio::test]
    async fn rejects_duplicate_nonce_after_dependency_restart() {
        let root = tempfile::tempdir().expect("root");
        let authorization = authorization(
            "mcp.server.list",
            json!({}),
            "cancel-list",
            "call-list",
            "nonce-restart",
        );
        dependency(root.path())
            .list_servers(authorization.clone())
            .await
            .expect("first use");

        let replay = dependency(root.path()).list_servers(authorization).await;

        assert_eq!(replay, Err(McpDependencyError::AuthorizationReplay));
    }

    #[tokio::test]
    async fn rejects_authorization_for_different_arguments() {
        let root = tempfile::tempdir().expect("root");
        let dependency = dependency(root.path());
        let authorization = authorization(
            "mcp.capabilities",
            json!({"server_id":"other"}),
            "cancel-capabilities",
            "call-capabilities",
            "nonce-mismatch",
        );

        let result = dependency.capabilities("docs", authorization).await;

        assert_eq!(result, Err(McpDependencyError::Authorization));
    }

    #[test]
    fn unsafe_stdio_environment_and_plain_http_are_rejected() {
        let root = tempfile::tempdir().expect("root");
        let bad = McpDependency::new(McpDependencyConfig {
            servers: vec![DependencyServerConfig {
                id: "bad".to_owned(),
                display_name: "Bad".to_owned(),
                active: true,
                transport: DependencyTransportConfig::StreamableHttp {
                    url: "http://example.com/mcp".to_owned(),
                    bearer_token_environment: None,
                },
            }],
            client_name: "agentmod".to_owned(),
            client_version: "0.1".to_owned(),
            request_timeout: Duration::from_secs(1),
            maximum_message_bytes: 1024,
            maximum_servers: 1,
            authorization_owner: "owner".into(),
            authorization_session: "session".into(),
            authorization_key_hex: encode_hex(&KEY),
            authorization_replay_root: root.path().join("replay"),
            http_state_root: root.path().join("http-state"),
        });
        assert!(matches!(bad, Err(McpDependencyError::InvalidConfiguration)));
    }

    #[test]
    fn persisted_http_state_is_bound_to_exact_server_owner_and_session() {
        let root = tempfile::tempdir().expect("root");
        let config = |url: &str, owner: &str, session: &str| McpDependencyConfig {
            servers: vec![DependencyServerConfig {
                id: "bound".into(),
                display_name: "Bound".into(),
                active: true,
                transport: DependencyTransportConfig::StreamableHttp {
                    url: url.to_owned(),
                    bearer_token_environment: None,
                },
            }],
            client_name: "agentmod".into(),
            client_version: "0.1.0".into(),
            request_timeout: Duration::from_secs(1),
            maximum_message_bytes: 1024,
            maximum_servers: 1,
            authorization_owner: owner.to_owned(),
            authorization_session: session.to_owned(),
            authorization_key_hex: encode_hex(&KEY),
            authorization_replay_root: root.path().join("replay"),
            http_state_root: root.path().join("http-state"),
        };
        let dependency = McpDependency::new(config("http://127.0.0.1:1/mcp", "owner", "session"))
            .expect("dependency");
        let server = dependency.server("bound").expect("server");
        write_http_state(
            server.http_state_path.as_deref().expect("state path"),
            &DurableHttpState {
                schema_version: 1,
                server_id: "bound".into(),
                server_identity: server.identity.clone(),
                owner: "owner".into(),
                authorization_session: "session".into(),
                protocol_version: Some(MCP_VERSION.into()),
                session_id: Some("mcp-session".into()),
                last_event_id: Some("event-1".into()),
                pending_request: Some(PendingHttpRequest {
                    request_id: "request-1".into(),
                    operation_hash: "a".repeat(64),
                }),
            },
        )
        .expect("write state");
        let state_path = server.http_state_path.clone().expect("state path");
        drop(server);
        drop(dependency);

        let backup = state_path.with_extension("backup");
        fs::rename(&state_path, &backup).expect("interrupted backup");
        fs::write(&state_path, b"truncated").expect("corrupt primary");
        let recovered = McpDependency::new(config("http://127.0.0.1:1/mcp", "owner", "session"))
            .expect("backup recovery");
        drop(recovered);
        assert!(state_path.exists());
        assert!(!backup.exists());
        assert!(matches!(
            McpDependency::new(config("http://127.0.0.1:2/mcp", "owner", "session")),
            Err(McpDependencyError::HttpState)
        ));
        assert!(matches!(
            McpDependency::new(config("http://127.0.0.1:1/mcp", "other", "session")),
            Err(McpDependencyError::HttpState)
        ));
        assert!(matches!(
            McpDependency::new(config("http://127.0.0.1:1/mcp", "owner", "other")),
            Err(McpDependencyError::HttpState)
        ));
        fs::write(&state_path, b"corrupt").expect("corrupt state");
        assert!(matches!(
            McpDependency::new(config("http://127.0.0.1:1/mcp", "owner", "session")),
            Err(McpDependencyError::HttpState)
        ));
    }

    #[tokio::test]
    async fn streamable_http_resumes_with_exact_session_and_event_cursor() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server_task = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.expect("POST");
            let first_request = read_test_http_request(&mut first).await;
            assert!(first_request.starts_with("POST /mcp "));
            let body = first_request
                .split_once("\r\n\r\n")
                .expect("request body")
                .1;
            let request: Value = serde_json::from_str(body).expect("request JSON");
            let request_id = request["id"].as_str().expect("request ID");
            let first_body = concat!(
                "id: event-1\n",
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",",
                "\"params\":{\"progressToken\":\"p\",\"progress\":1}}\n\n"
            );
            write_test_http_response(
                &mut first,
                first_body,
                &[
                    ("content-type", "text/event-stream"),
                    ("mcp-session-id", "s-1"),
                ],
            )
            .await;

            let (mut second, _) = listener.accept().await.expect("resume GET");
            let second_request = read_test_http_request(&mut second).await;
            let lower = second_request.to_ascii_lowercase();
            assert!(second_request.starts_with("GET /mcp "));
            assert!(lower.contains("\r\nlast-event-id: event-1\r\n"));
            assert!(lower.contains("\r\nmcp-session-id: s-1\r\n"));
            assert!(lower.contains("\r\nmcp-protocol-version: 2025-06-18\r\n"));
            let second_body = format!(
                "id: event-2\ndata: {{\"jsonrpc\":\"2.0\",\"id\":\"{request_id}\",\"result\":{{\"value\":\"resumed\"}}}}\n\n"
            );
            write_test_http_response(
                &mut second,
                &second_body,
                &[
                    ("content-type", "text/event-stream"),
                    ("mcp-session-id", "s-1"),
                ],
            )
            .await;
        });
        let root = tempfile::tempdir().expect("root");
        let dependency = McpDependency::new(McpDependencyConfig {
            servers: vec![DependencyServerConfig {
                id: "http-fixture".into(),
                display_name: "HTTP fixture".into(),
                active: true,
                transport: DependencyTransportConfig::StreamableHttp {
                    url: format!("http://{address}/mcp"),
                    bearer_token_environment: None,
                },
            }],
            client_name: "agentmod".into(),
            client_version: "0.1.0".into(),
            request_timeout: Duration::from_secs(2),
            maximum_message_bytes: 64 * 1024,
            maximum_servers: 1,
            authorization_owner: "owner".into(),
            authorization_session: "session".into(),
            authorization_key_hex: encode_hex(&KEY),
            authorization_replay_root: root.path().join("replay"),
            http_state_root: root.path().join("http-state"),
        })
        .expect("dependency");
        let server = dependency.server("http-fixture").expect("server");
        server.state.lock().await.protocol_version = Some(MCP_VERSION.into());
        let (result, progress) = dependency
            .request(&server, "fixture/resume", json!({}), None)
            .await
            .expect("resumed response");
        assert_eq!(result["value"], "resumed");
        assert_eq!(progress.len(), 1);
        assert_eq!(
            server.state.lock().await.last_event_id.as_deref(),
            Some("event-2")
        );
        server_task.await.expect("server task");
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the restart fixture asserts every POST/GET cursor boundary in one server lifecycle"
    )]
    async fn streamable_http_pending_cursor_resumes_after_dependency_restart() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server_task = tokio::spawn(async move {
            let mut request_id = String::new();
            for index in 1..=4 {
                let (mut connection, _) = listener.accept().await.expect("connection");
                let request = read_test_http_request(&mut connection).await;
                if index == 1 {
                    assert!(request.starts_with("POST /mcp "));
                    let body = request.split_once("\r\n\r\n").expect("body").1;
                    let message: Value = serde_json::from_str(body).expect("request JSON");
                    request_id = message["id"].as_str().expect("request ID").to_owned();
                } else {
                    assert!(request.starts_with("GET /mcp "));
                    let lower = request.to_ascii_lowercase();
                    assert!(
                        lower.contains(&format!("\r\nlast-event-id: restart-{}\r\n", index - 1))
                    );
                    assert!(lower.contains("\r\nmcp-session-id: restart-session\r\n"));
                    assert!(lower.contains("\r\nmcp-protocol-version: 2025-06-18\r\n"));
                }
                let body = format!(
                    "id: restart-{index}\ndata: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{{\"progress\":{index}}}}}\n\n"
                );
                write_test_http_response(
                    &mut connection,
                    &body,
                    &[
                        ("content-type", "text/event-stream"),
                        ("mcp-session-id", "restart-session"),
                    ],
                )
                .await;
            }

            let (mut recovered, _) = listener.accept().await.expect("recovered GET");
            let recovered_request = read_test_http_request(&mut recovered).await;
            let lower = recovered_request.to_ascii_lowercase();
            assert!(recovered_request.starts_with("GET /mcp "));
            assert!(lower.contains("\r\nlast-event-id: restart-4\r\n"));
            assert!(lower.contains("\r\nmcp-session-id: restart-session\r\n"));
            assert!(lower.contains("\r\nmcp-protocol-version: 2025-06-18\r\n"));
            let body = format!(
                "id: restart-5\ndata: {{\"jsonrpc\":\"2.0\",\"id\":\"{request_id}\",\"result\":{{\"value\":\"recovered\"}}}}\n\n"
            );
            write_test_http_response(
                &mut recovered,
                &body,
                &[
                    ("content-type", "text/event-stream"),
                    ("mcp-session-id", "restart-session"),
                ],
            )
            .await;
        });
        let root = tempfile::tempdir().expect("root");
        let make_dependency = || {
            McpDependency::new(McpDependencyConfig {
                servers: vec![DependencyServerConfig {
                    id: "restart-fixture".into(),
                    display_name: "Restart fixture".into(),
                    active: true,
                    transport: DependencyTransportConfig::StreamableHttp {
                        url: format!("http://{address}/mcp"),
                        bearer_token_environment: None,
                    },
                }],
                client_name: "agentmod".into(),
                client_version: "0.1.0".into(),
                request_timeout: Duration::from_secs(2),
                maximum_message_bytes: 64 * 1024,
                maximum_servers: 1,
                authorization_owner: "owner".into(),
                authorization_session: "session".into(),
                authorization_key_hex: encode_hex(&KEY),
                authorization_replay_root: root.path().join("replay"),
                http_state_root: root.path().join("http-state"),
            })
            .expect("dependency")
        };
        let dependency = make_dependency();
        let server = dependency.server("restart-fixture").expect("server");
        server.state.lock().await.protocol_version = Some(MCP_VERSION.into());
        assert_eq!(
            dependency
                .request(&server, "fixture/restart", json!({"value":1}), None)
                .await,
            Err(McpDependencyError::ResumeExhausted)
        );
        drop(server);
        drop(dependency);

        let recovered = make_dependency();
        let server = recovered.server("restart-fixture").expect("server");
        assert_eq!(
            recovered
                .request(&server, "fixture/other", json!({"value":1}), None)
                .await,
            Err(McpDependencyError::CursorConflict)
        );
        let (result, progress) = recovered
            .request(&server, "fixture/restart", json!({"value":1}), None)
            .await
            .expect("recovered result");
        assert_eq!(result["value"], "recovered");
        assert!(progress.is_empty());
        let state = server.state.lock().await;
        assert_eq!(state.last_event_id.as_deref(), Some("restart-5"));
        assert!(state.pending_request.is_none());
        drop(state);
        server_task.await.expect("server task");
    }

    #[test]
    fn multi_event_sse_preserves_progress_cursor_and_terminal_result() {
        let parsed = parse_sse_response(
            br#"id: event-41
event: message
data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"p","progress":1}}

id: event-42
event: message
data: {"jsonrpc":"2.0","id":"request-1","result":{"content":[{"type":"text","text":"done"}]}}

"#,
            "request-1",
            None,
        )
        .expect("SSE");
        assert_eq!(parsed.last_event_id.as_deref(), Some("event-42"));
        assert_eq!(parsed.progress.len(), 1);
        assert_eq!(parsed.progress[0].token, Some(json!("p")));
        assert_eq!(
            parsed.result.expect("terminal")["content"][0]["text"],
            "done"
        );
    }

    #[test]
    fn sse_duplicate_cursor_is_suppressed_before_progress_projection() {
        let parsed = parse_sse_response(
            br#"id: event-41
data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":41}}

id: event-42
data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":42}}

"#,
            "request-duplicate",
            Some("event-41"),
        )
        .expect("SSE");
        assert_eq!(parsed.last_event_id.as_deref(), Some("event-42"));
        assert_eq!(parsed.progress.len(), 1);
        assert_eq!(parsed.progress[0].value["progress"], 42);
    }

    #[test]
    fn progress_only_sse_retains_resume_cursor_without_fabricating_result() {
        let parsed = parse_sse_response(
            br#"id: resumable-7
data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":7}}

"#,
            "request-2",
            None,
        )
        .expect("SSE");
        assert_eq!(parsed.last_event_id.as_deref(), Some("resumable-7"));
        assert_eq!(parsed.progress.len(), 1);
        assert!(parsed.result.is_none());
    }

    #[test]
    fn sse_remote_error_for_selected_request_is_not_treated_as_resumable() {
        assert_eq!(
            parse_sse_response(
                br#"id: failed-1
data: {"jsonrpc":"2.0","id":"request-3","error":{"code":-32603,"message":"failed"}}

"#,
                "request-3",
                None,
            )
            .err(),
            Some(McpDependencyError::RemoteError)
        );
    }

    async fn read_test_http_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let count = stream.read(&mut chunk).await.expect("read request");
            assert!(count > 0, "request ended before headers");
            bytes.extend_from_slice(&chunk[..count]);
            let Some(header_end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") else {
                continue;
            };
            let header_end = header_end + 4;
            let headers = std::str::from_utf8(&bytes[..header_end]).expect("headers");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if bytes.len() >= header_end + content_length {
                return String::from_utf8(bytes[..header_end + content_length].to_vec())
                    .expect("request");
            }
        }
    }

    async fn write_test_http_response(
        stream: &mut tokio::net::TcpStream,
        body: &str,
        headers: &[(&str, &str)],
    ) {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n",
            body.len()
        );
        for (name, value) in headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str("\r\n");
        response.push_str(body);
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        stream.shutdown().await.expect("shutdown response");
    }
}
