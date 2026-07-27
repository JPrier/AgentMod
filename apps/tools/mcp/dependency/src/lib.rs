//! MCP transport, lifecycle, capability, and protocol adapters.

use std::{
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
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

#[derive(Default)]
struct RuntimeState {
    initialized: bool,
    session_id: Option<String>,
    last_event_id: Option<String>,
    stdio: Option<StdioConnection>,
}

struct Server {
    config: DependencyServerConfig,
    state: Mutex<RuntimeState>,
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
        {
            return Err(McpDependencyError::InvalidConfiguration);
        }
        let authorization_key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| McpDependencyError::InvalidConfiguration)?;
        config.authorization_key_hex.clear();
        std::fs::create_dir_all(&config.authorization_replay_root)
            .map_err(|_| McpDependencyError::ReplayState)?;
        let mut servers = BTreeMap::new();
        for server in &config.servers {
            validate_server(server)?;
            if servers
                .insert(
                    server.id.clone(),
                    Arc::new(Server {
                        config: server.clone(),
                        state: Mutex::new(RuntimeState::default()),
                    }),
                )
                .is_some()
            {
                return Err(McpDependencyError::InvalidConfiguration);
            }
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
            server.state.lock().await.initialized = true;
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

    async fn http_request(
        &self,
        server: &Server,
        request: Value,
    ) -> Result<(Value, Vec<DependencyProgress>), McpDependencyError> {
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
        let mut builder = self
            .client
            .post(url)
            .header("accept", "application/json, text/event-stream")
            .json(&request);
        if let Some(session_id) = server.state.lock().await.session_id.clone() {
            builder = builder.header("mcp-session-id", session_id);
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
        update_http_session(server, &response).await;
        if !response.status().is_success() {
            return Err(McpDependencyError::RemoteError);
        }
        let mut parsed =
            read_http_response(response, request_id, self.config.maximum_message_bytes).await?;
        if let Some(event_id) = &parsed.last_event_id {
            server.state.lock().await.last_event_id = Some(event_id.clone());
        }
        if let Some(result) = parsed.result {
            return Ok((result, parsed.progress));
        }
        let mut last_event_id = parsed
            .last_event_id
            .or_else(|| server.state.try_lock().ok()?.last_event_id.clone())
            .ok_or(McpDependencyError::Protocol)?;
        for _ in 0..HTTP_RESUME_ATTEMPTS {
            let mut resume = self
                .client
                .get(url)
                .header("accept", "text/event-stream")
                .header("last-event-id", &last_event_id);
            if let Some(session_id) = server.state.lock().await.session_id.clone() {
                resume = resume.header("mcp-session-id", session_id);
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
            update_http_session(server, &response).await;
            if !response.status().is_success() {
                return Err(McpDependencyError::RemoteError);
            }
            let resumed =
                read_http_response(response, request_id, self.config.maximum_message_bytes).await?;
            parsed.progress.extend(resumed.progress);
            if let Some(event_id) = resumed.last_event_id {
                last_event_id.clone_from(&event_id);
                server.state.lock().await.last_event_id = Some(event_id);
            }
            if let Some(result) = resumed.result {
                return Ok((result, parsed.progress));
            }
        }
        Err(McpDependencyError::ResumeExhausted)
    }
}

struct ParsedHttpResponse {
    result: Option<Value>,
    progress: Vec<DependencyProgress>,
    last_event_id: Option<String>,
}

async fn update_http_session(server: &Server, response: &reqwest::Response) {
    if let Some(session) = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
    {
        let mut state = server.state.lock().await;
        if state.session_id.as_deref() != Some(session) {
            state.last_event_id = None;
        }
        state.session_id = Some(session.to_owned());
    }
}

async fn read_http_response(
    response: reqwest::Response,
    request_id: &str,
    maximum_message_bytes: usize,
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
        parse_sse_response(&bytes, request_id)
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
) -> Result<ParsedHttpResponse, McpDependencyError> {
    let text = std::str::from_utf8(bytes).map_err(|_| McpDependencyError::Protocol)?;
    let mut result = None;
    let mut progress = Vec::new();
    let mut last_event_id = None;
    let mut event_id = None;
    let mut data = Vec::new();
    for line in text.lines().chain(std::iter::once("")) {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            if data.is_empty() {
                event_id = None;
            } else {
                let value: Value = serde_json::from_str(&data.join("\n"))
                    .map_err(|_| McpDependencyError::Protocol)?;
                if let Some(id) = event_id.take().filter(|id: &String| !id.is_empty()) {
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
    if result.is_none() && progress.is_empty() && last_event_id.is_none() {
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
            authorization_replay_root: PathBuf::from("replay"),
        });
        assert!(matches!(bad, Err(McpDependencyError::InvalidConfiguration)));
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
        })
        .expect("dependency");
        let server = dependency.server("http-fixture").expect("server");
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
    fn progress_only_sse_retains_resume_cursor_without_fabricating_result() {
        let parsed = parse_sse_response(
            br#"id: resumable-7
data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":7}}

"#,
            "request-2",
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
