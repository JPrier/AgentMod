//! Supervised local tool-host adapters using the shared tool protocol.
#![allow(
    missing_docs,
    reason = "dependency-local tool transport records are self-describing"
)]

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
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
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::timeout,
};

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyToolCommand {
    pub execution_id: String,
    pub receipt_only: bool,
    pub session_id: String,
    pub workspace: PathBuf,
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCancelToolRequest {
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyToolReceipt {
    pub command: DependencyToolCommand,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DependencyToolEvent {
    Started {
        call_id: String,
    },
    Progress {
        call_id: String,
        message: String,
        completed: Option<u64>,
        total: Option<u64>,
    },
    Output {
        call_id: String,
        stream: DependencyOutputStream,
        content: String,
    },
    Completed {
        call_id: String,
        result: Value,
        artifact: Option<String>,
        truncated: bool,
    },
    Failed {
        call_id: String,
        code: String,
        message: String,
        retryable: bool,
    },
    Cancelled {
        call_id: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DependencyOutputStream {
    Standard,
    Error,
}

#[derive(Clone, Debug)]
pub struct ToolHostDependencyConfig {
    pub kind: ToolHostKind,
    pub program: String,
    pub arguments: Vec<String>,
    pub owner: String,
    pub state_root: Option<PathBuf>,
    pub maximum_frame_bytes: usize,
    pub request_timeout: Duration,
    pub authorization_key: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolHostKind {
    Browser,
    Filesystem,
    Git,
    Lsp,
    Mcp,
    Web,
}

struct Connection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Clone)]
pub struct ProcessToolHostDependency {
    config: Arc<ToolHostDependencyConfig>,
    connections: Arc<Mutex<HashMap<String, Connection>>>,
}

#[async_trait]
pub trait ToolHostDependencyPort: Send + Sync {
    async fn execute(
        &self,
        command: DependencyToolCommand,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError>;
    async fn cancel(
        &self,
        _request: DependencyCancelToolRequest,
    ) -> Result<bool, ToolHostDependencyError> {
        Ok(false)
    }
    /// Lists verified durable terminal receipts exposed by this dependency.
    ///
    /// # Errors
    ///
    /// Returns a dependency-owned error when receipt storage is unavailable or
    /// a stored receipt fails integrity validation.
    fn list_receipts(&self) -> Result<Vec<DependencyToolReceipt>, ToolHostDependencyError> {
        Ok(Vec::new())
    }
    async fn shutdown(&self);
}

impl ProcessToolHostDependency {
    #[must_use]
    pub fn generate_authorization_key() -> [u8; 32] {
        let first = uuid::Uuid::now_v7();
        let second = uuid::Uuid::now_v7();
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(first.as_bytes());
        key[16..].copy_from_slice(second.as_bytes());
        key
    }

    /// Creates a lazy per-session capability-host supervisor.
    ///
    /// # Errors
    ///
    /// Returns [`ToolHostDependencyError::InvalidConfiguration`] when process,
    /// transport, owner, or authorization settings are unsafe.
    pub fn new(config: ToolHostDependencyConfig) -> Result<Self, ToolHostDependencyError> {
        if config.program.trim().is_empty()
            || config.program.contains('\0')
            || config.arguments.iter().any(|value| value.contains('\0'))
            || config.owner.trim().is_empty()
            || (matches!(
                config.kind,
                ToolHostKind::Browser | ToolHostKind::Git | ToolHostKind::Mcp | ToolHostKind::Web
            ) && config
                .state_root
                .as_ref()
                .is_none_or(|path| path.as_os_str().is_empty()))
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

    #[allow(
        clippy::too_many_lines,
        reason = "host bootstrap keeps every capability-specific secret and state mapping explicit"
    )]
    fn connect(
        &self,
        session_id: &str,
        workspace: &std::path::Path,
    ) -> Result<Connection, ToolHostDependencyError> {
        let mut command = Command::new(&self.config.program);
        command
            .args(&self.config.arguments)
            .current_dir(workspace)
            .env_clear()
            .envs(host_environment())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let authorization_key = encode_hex(&self.config.authorization_key);
        match self.config.kind {
            ToolHostKind::Browser => {
                let session_id = uuid::Uuid::parse_str(session_id)
                    .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
                let artifact_root = self
                    .config
                    .state_root
                    .as_ref()
                    .ok_or(ToolHostDependencyError::InvalidConfiguration)?
                    .join(session_id.to_string())
                    .join("artifacts")
                    .join("browser");
                command
                    .env("AGENTMOD_BROWSER_AUTH_KEY", authorization_key)
                    .env("AGENTMOD_BROWSER_OWNER", &self.config.owner)
                    .env("AGENTMOD_BROWSER_SESSION", session_id.to_string())
                    .env("AGENTMOD_BROWSER_ARTIFACT_ROOT", artifact_root);
                for variable in [
                    "AGENTMOD_BROWSER_WEBDRIVER_URL",
                    "AGENTMOD_BROWSER_NAME",
                    "AGENTMOD_BROWSER_ALLOWED_DOMAINS",
                    "AGENTMOD_BROWSER_ALLOW_LOOPBACK",
                ] {
                    if let Some(value) = std::env::var_os(variable) {
                        command.env(variable, value);
                    }
                }
            }
            ToolHostKind::Filesystem => {
                command
                    .env("AGENTMOD_FILESYSTEM_AUTH_KEY_HEX", authorization_key)
                    .env("AGENTMOD_FILESYSTEM_AUTH_OWNER", &self.config.owner)
                    .env("AGENTMOD_FILESYSTEM_AUTH_SESSION", session_id);
            }
            ToolHostKind::Git => {
                let session_id = uuid::Uuid::parse_str(session_id)
                    .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
                let artifact_root = self
                    .config
                    .state_root
                    .as_ref()
                    .ok_or(ToolHostDependencyError::InvalidConfiguration)?
                    .join(session_id.to_string())
                    .join("artifacts")
                    .join("git");
                command
                    .env("AGENTMOD_GIT_AUTH_KEY", authorization_key)
                    .env("AGENTMOD_GIT_OWNER", &self.config.owner)
                    .env("AGENTMOD_GIT_SESSION", session_id.to_string())
                    .env("AGENTMOD_GIT_ARTIFACT_ROOT", artifact_root);
            }
            ToolHostKind::Web => {
                let session_id = uuid::Uuid::parse_str(session_id)
                    .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
                let artifact_root = self
                    .config
                    .state_root
                    .as_ref()
                    .ok_or(ToolHostDependencyError::InvalidConfiguration)?
                    .join(session_id.to_string())
                    .join("artifacts")
                    .join("web");
                command
                    .env("AGENTMOD_WEB_AUTH_KEY", authorization_key)
                    .env("AGENTMOD_WEB_OWNER", &self.config.owner)
                    .env("AGENTMOD_WEB_SESSION", session_id.to_string())
                    .env("AGENTMOD_WEB_ARTIFACT_ROOT", artifact_root);
            }
            ToolHostKind::Lsp => {
                command
                    .env("AGENTMOD_LSP_AUTH_KEY_HEX", authorization_key)
                    .env("AGENTMOD_LSP_AUTH_OWNER", &self.config.owner)
                    .env("AGENTMOD_LSP_AUTH_SESSION", session_id);
                if let Some(servers) = std::env::var_os("AGENTMOD_LSP_SERVERS_JSON") {
                    command.env("AGENTMOD_LSP_SERVERS_JSON", servers);
                }
            }
            ToolHostKind::Mcp => {
                let session_id = uuid::Uuid::parse_str(session_id)
                    .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
                let replay_root = self
                    .config
                    .state_root
                    .as_ref()
                    .ok_or(ToolHostDependencyError::InvalidConfiguration)?
                    .join(session_id.to_string())
                    .join("artifacts")
                    .join("mcp")
                    .join("authorization-replay");
                command
                    .env("AGENTMOD_MCP_AUTH_KEY", authorization_key)
                    .env("AGENTMOD_MCP_OWNER", &self.config.owner)
                    .env("AGENTMOD_MCP_SESSION", session_id.to_string())
                    .env("AGENTMOD_MCP_REPLAY_ROOT", replay_root);
                if let Some(servers) = std::env::var_os("AGENTMOD_MCP_SERVERS_JSON") {
                    command.env("AGENTMOD_MCP_SERVERS_JSON", servers);
                }
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
        let tool = canonical_tool_name(self.config.kind, &command.tool)?;
        let digest = canonical_tool_digest(
            self.config.kind,
            tool,
            &command.arguments,
            &command.cancellation_id,
        )?;
        let cancellation_id = CancellationId::from_str(&command.cancellation_id)
            .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
        let grant = self.grant(&command, tool, digest)?;
        let wire = ToolHostCommand::Execute {
            call_id: command.call_id.clone(),
            tool: tool.into(),
            arguments: command.arguments,
            normalized_digest: digest.to_hex(),
            authorization_grant: grant,
            cancellation_id,
        };
        let connection_key = format!(
            "{}\0{}",
            command.session_id,
            command.workspace.to_string_lossy()
        );
        let mut connections = self.connections.lock().await;
        if !connections.contains_key(&connection_key) {
            let connection = self.connect(&command.session_id, &command.workspace)?;
            connections.insert(connection_key.clone(), connection);
        }
        let connection = connections
            .get_mut(&connection_key)
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
        tool: &str,
        digest: ContentHash,
    ) -> Result<String, ToolHostDependencyError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ToolHostDependencyError::Clock)?;
        if self.config.kind == ToolHostKind::Lsp {
            let expires_at = now.as_secs().saturating_add(30);
            let nonce = uuid::Uuid::now_v7();
            let claims = format!(
                "v1|{}|{}|{}|{}|{}|{}",
                self.config.owner,
                command.session_id,
                command.call_id,
                expires_at,
                nonce,
                digest.to_hex()
            );
            let signature = blake3::keyed_hash(&self.config.authorization_key, claims.as_bytes());
            return Ok(format!("{claims}|{}", signature.to_hex()));
        }
        let issued_at =
            i64::try_from(now.as_millis()).map_err(|_| ToolHostDependencyError::Clock)?;
        seal_authorization(
            &AuthorizationClaims {
                owner: self.config.owner.clone(),
                session: command.session_id.clone(),
                call_id: command.call_id.clone(),
                action: tool.into(),
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
impl ToolHostDependencyPort for ProcessToolHostDependency {
    async fn execute(
        &self,
        command: DependencyToolCommand,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError> {
        let key = format!(
            "{}\0{}",
            command.session_id,
            command.workspace.to_string_lossy()
        );
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

fn host_environment() -> impl Iterator<Item = (String, std::ffi::OsString)> {
    const ALLOWED: &[&str] = &[
        "PATH",
        "PATHEXT",
        "SystemRoot",
        "WINDIR",
        "TEMP",
        "TMP",
        "TMPDIR",
        "HOME",
        "USERPROFILE",
    ];
    ALLOWED
        .iter()
        .filter_map(|key| std::env::var_os(key).map(|value| ((*key).to_owned(), value)))
}

fn validate(command: &DependencyToolCommand) -> Result<(), ToolHostDependencyError> {
    if command.execution_id.trim().is_empty()
        || command.execution_id.len() > 512
        || command.session_id.trim().is_empty()
        || command.workspace.as_os_str().is_empty()
        || command.call_id.trim().is_empty()
        || command.tool.trim().is_empty()
        || !command.arguments.is_object()
        || command.cancellation_id.trim().is_empty()
    {
        return Err(ToolHostDependencyError::InvalidRequest);
    }
    Ok(())
}

fn canonical_tool_name(
    kind: ToolHostKind,
    tool: &str,
) -> Result<&'static str, ToolHostDependencyError> {
    match (kind, tool) {
        (
            ToolHostKind::Browser,
            "browser.start" | "browser.navigate" | "browser.inspect" | "browser.screenshot"
            | "browser.click" | "browser.type" | "browser.submit" | "browser.download"
            | "browser.close",
        ) => Ok(match tool {
            "browser.start" => "browser.start",
            "browser.navigate" => "browser.navigate",
            "browser.inspect" => "browser.inspect",
            "browser.screenshot" => "browser.screenshot",
            "browser.click" => "browser.click",
            "browser.type" => "browser.type",
            "browser.submit" => "browser.submit",
            "browser.download" => "browser.download",
            "browser.close" => "browser.close",
            _ => unreachable!("outer pattern limits browser tool names"),
        }),
        (ToolHostKind::Filesystem, "read_file" | "filesystem.read") => Ok("filesystem.read"),
        (ToolHostKind::Filesystem, "list_files" | "filesystem.list") => Ok("filesystem.list"),
        (ToolHostKind::Filesystem, "glob" | "filesystem.glob") => Ok("filesystem.glob"),
        (ToolHostKind::Filesystem, "grep" | "filesystem.grep") => Ok("filesystem.grep"),
        (ToolHostKind::Filesystem, "write_file" | "filesystem.write") => Ok("filesystem.write"),
        (ToolHostKind::Filesystem, "edit_file" | "filesystem.edit") => Ok("filesystem.edit"),
        (ToolHostKind::Filesystem, "apply_patch" | "filesystem.apply_patch") => {
            Ok("filesystem.apply_patch")
        }
        (
            ToolHostKind::Git,
            "git.discover"
            | "git.status"
            | "git.diff"
            | "git.changed_files"
            | "git.branch"
            | "git.dirty"
            | "git.worktree_create"
            | "git.worktree_cleanup"
            | "git.checkpoint_create"
            | "git.checkpoint_restore"
            | "git.export_patch",
        ) => Ok(match tool {
            "git.discover" => "git.discover",
            "git.status" => "git.status",
            "git.diff" => "git.diff",
            "git.changed_files" => "git.changed_files",
            "git.branch" => "git.branch",
            "git.dirty" => "git.dirty",
            "git.worktree_create" => "git.worktree_create",
            "git.worktree_cleanup" => "git.worktree_cleanup",
            "git.checkpoint_create" => "git.checkpoint_create",
            "git.checkpoint_restore" => "git.checkpoint_restore",
            "git.export_patch" => "git.export_patch",
            _ => unreachable!("outer pattern limits Git tool names"),
        }),
        (ToolHostKind::Web, "http.request") => Ok("http.request"),
        (ToolHostKind::Web, "web.fetch") => Ok("web.fetch"),
        (ToolHostKind::Web, "web.search") => Ok("web.search"),
        (
            ToolHostKind::Lsp,
            "lsp.project_root"
            | "lsp.diagnostics"
            | "lsp.document_symbols"
            | "lsp.workspace_symbols"
            | "lsp.definition"
            | "lsp.references"
            | "lsp.hover"
            | "lsp.signature_help"
            | "lsp.rename"
            | "lsp.formatting"
            | "lsp.code_actions",
        ) => Ok(match tool {
            "lsp.project_root" => "lsp.project_root",
            "lsp.diagnostics" => "lsp.diagnostics",
            "lsp.document_symbols" => "lsp.document_symbols",
            "lsp.workspace_symbols" => "lsp.workspace_symbols",
            "lsp.definition" => "lsp.definition",
            "lsp.references" => "lsp.references",
            "lsp.hover" => "lsp.hover",
            "lsp.signature_help" => "lsp.signature_help",
            "lsp.rename" => "lsp.rename",
            "lsp.formatting" => "lsp.formatting",
            "lsp.code_actions" => "lsp.code_actions",
            _ => unreachable!("outer pattern limits LSP tool names"),
        }),
        (ToolHostKind::Mcp, "mcp.server.list") => Ok("mcp.server.list"),
        (ToolHostKind::Mcp, "mcp.capabilities") => Ok("mcp.capabilities"),
        (ToolHostKind::Mcp, "mcp.invoke") => Ok("mcp.invoke"),
        _ => Err(ToolHostDependencyError::UnsupportedTool),
    }
}

fn canonical_tool_digest(
    kind: ToolHostKind,
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<ContentHash, ToolHostDependencyError> {
    match kind {
        ToolHostKind::Browser => canonical_browser_digest(tool, arguments, cancellation_id),
        ToolHostKind::Filesystem => canonical_filesystem_digest(tool, arguments),
        ToolHostKind::Git => canonical_git_digest(tool, arguments),
        ToolHostKind::Lsp => canonical_lsp_digest(tool, arguments),
        ToolHostKind::Mcp => canonical_mcp_digest(tool, arguments, cancellation_id),
        ToolHostKind::Web => canonical_web_digest(tool, arguments, cancellation_id),
    }
}

fn canonical_browser_digest(
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<ContentHash, ToolHostDependencyError> {
    let object = arguments
        .as_object()
        .ok_or(ToolHostDependencyError::InvalidRequest)?;
    let expanded = match tool {
        "browser.start" | "browser.screenshot" | "browser.close" => {
            reject_unknown(object, &[])?;
            json!({})
        }
        "browser.navigate" => {
            reject_unknown(object, &["url"])?;
            json!({"url":required_value(object, "url")?})
        }
        "browser.inspect" => {
            reject_unknown(object, &["maximum_bytes"])?;
            json!({
                "maximum_bytes":object
                    .get("maximum_bytes")
                    .cloned()
                    .unwrap_or_else(|| json!(128 * 1024)),
            })
        }
        "browser.click" | "browser.submit" => {
            reject_unknown(object, &["selector"])?;
            json!({"selector":required_value(object, "selector")?})
        }
        "browser.type" => {
            reject_unknown(object, &["selector", "text"])?;
            json!({
                "selector":required_value(object, "selector")?,
                "text":required_value(object, "text")?,
            })
        }
        "browser.download" => {
            reject_unknown(object, &["url", "maximum_bytes"])?;
            json!({
                "url":required_value(object, "url")?,
                "maximum_bytes":object
                    .get("maximum_bytes")
                    .cloned()
                    .unwrap_or_else(|| json!(32 * 1024 * 1024)),
            })
        }
        _ => return Err(ToolHostDependencyError::UnsupportedTool),
    };
    let bytes = serde_json::to_vec(&(tool, normalize_json(&expanded), cancellation_id))
        .map_err(|_| ToolHostDependencyError::Protocol)?;
    Ok(ContentHash::digest(&bytes))
}

fn canonical_git_digest(
    tool: &str,
    arguments: &Value,
) -> Result<ContentHash, ToolHostDependencyError> {
    let normalized = normalize_json(arguments);
    let bytes =
        serde_json::to_vec(&(tool, normalized)).map_err(|_| ToolHostDependencyError::Protocol)?;
    Ok(ContentHash::digest(&bytes))
}

fn canonical_web_digest(
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<ContentHash, ToolHostDependencyError> {
    let object = arguments
        .as_object()
        .ok_or(ToolHostDependencyError::InvalidRequest)?;
    let expanded = match tool {
        "http.request" => {
            reject_unknown(
                object,
                &[
                    "method",
                    "url",
                    "query",
                    "headers",
                    "body",
                    "max_redirects",
                    "timeout_ms",
                    "max_response_bytes",
                    "max_inline_bytes",
                ],
            )?;
            json!({
                "method": required_value(object, "method")?,
                "url": required_value(object, "url")?,
                "query": object.get("query").cloned().unwrap_or_else(|| json!({})),
                "headers": object.get("headers").cloned().unwrap_or_else(|| json!({})),
                "body": object.get("body").cloned().unwrap_or_else(|| json!({"kind":"empty"})),
                "max_redirects": object.get("max_redirects").cloned().unwrap_or_else(|| json!(5)),
                "timeout_ms": object.get("timeout_ms").cloned().unwrap_or_else(|| json!(30_000)),
                "max_response_bytes": object.get("max_response_bytes").cloned().unwrap_or_else(|| json!(8 * 1024 * 1024)),
                "max_inline_bytes": object.get("max_inline_bytes").cloned().unwrap_or_else(|| json!(64 * 1024)),
            })
        }
        "web.fetch" => {
            reject_unknown(
                object,
                &[
                    "url",
                    "max_redirects",
                    "timeout_ms",
                    "max_response_bytes",
                    "max_inline_bytes",
                    "use_cache",
                ],
            )?;
            json!({
                "url": required_value(object, "url")?,
                "max_redirects": object.get("max_redirects").cloned().unwrap_or_else(|| json!(5)),
                "timeout_ms": object.get("timeout_ms").cloned().unwrap_or_else(|| json!(30_000)),
                "max_response_bytes": object.get("max_response_bytes").cloned().unwrap_or_else(|| json!(8 * 1024 * 1024)),
                "max_inline_bytes": object.get("max_inline_bytes").cloned().unwrap_or_else(|| json!(64 * 1024)),
                "use_cache": object.get("use_cache").cloned().unwrap_or(Value::Bool(true)),
            })
        }
        "web.search" => {
            reject_unknown(
                object,
                &[
                    "query",
                    "count",
                    "freshness",
                    "domain_allowlist",
                    "domain_denylist",
                    "language",
                    "locale",
                    "timeout_ms",
                ],
            )?;
            json!({
                "query": required_value(object, "query")?,
                "count": object.get("count").cloned().unwrap_or_else(|| json!(10)),
                "freshness": object.get("freshness").cloned().unwrap_or(Value::Null),
                "domain_allowlist": object.get("domain_allowlist").cloned().unwrap_or_else(|| json!([])),
                "domain_denylist": object.get("domain_denylist").cloned().unwrap_or_else(|| json!([])),
                "language": object.get("language").cloned().unwrap_or(Value::Null),
                "locale": object.get("locale").cloned().unwrap_or(Value::Null),
                "timeout_ms": object.get("timeout_ms").cloned().unwrap_or_else(|| json!(30_000)),
            })
        }
        _ => return Err(ToolHostDependencyError::UnsupportedTool),
    };
    let normalized = normalize_json(&expanded);
    let bytes = serde_json::to_vec(&(tool, cancellation_id, normalized))
        .map_err(|_| ToolHostDependencyError::Protocol)?;
    Ok(ContentHash::digest(&bytes))
}

fn canonical_lsp_digest(
    tool: &str,
    arguments: &Value,
) -> Result<ContentHash, ToolHostDependencyError> {
    let object = arguments
        .as_object()
        .ok_or(ToolHostDependencyError::InvalidRequest)?;
    let operation = match tool {
        "lsp.project_root" => {
            reject_unknown(object, &["document"])?;
            json!({"operation":"project_root","path":required_value(object, "document")?})
        }
        "lsp.diagnostics" => {
            reject_unknown(object, &["document"])?;
            json!({"operation":"diagnostics","document":required_value(object, "document")?})
        }
        "lsp.document_symbols" => {
            reject_unknown(object, &["document"])?;
            json!({"operation":"document_symbols","document":required_value(object, "document")?})
        }
        "lsp.workspace_symbols" => {
            reject_unknown(object, &["query"])?;
            json!({"operation":"workspace_symbols","query":object.get("query").cloned().unwrap_or_else(|| json!(""))})
        }
        "lsp.definition" | "lsp.hover" | "lsp.signature_help" => {
            reject_unknown(object, &["document", "position"])?;
            json!({
                "operation": tool.trim_start_matches("lsp."),
                "document": required_value(object, "document")?,
                "position": required_value(object, "position")?,
            })
        }
        "lsp.references" => {
            reject_unknown(object, &["document", "position", "include_declaration"])?;
            json!({
                "operation":"references",
                "document":required_value(object, "document")?,
                "position":required_value(object, "position")?,
                "include_declaration":object.get("include_declaration").cloned().unwrap_or(Value::Bool(false)),
            })
        }
        "lsp.rename" => {
            reject_unknown(object, &["document", "position", "new_name"])?;
            json!({
                "operation":"rename",
                "document":required_value(object, "document")?,
                "position":required_value(object, "position")?,
                "new_name":required_value(object, "new_name")?,
            })
        }
        "lsp.formatting" => {
            reject_unknown(object, &["document", "tab_size", "insert_spaces"])?;
            json!({
                "operation":"formatting",
                "document":required_value(object, "document")?,
                "tab_size":object.get("tab_size").cloned().unwrap_or_else(|| json!(4)),
                "insert_spaces":object.get("insert_spaces").cloned().unwrap_or(Value::Bool(true)),
            })
        }
        "lsp.code_actions" => {
            reject_unknown(object, &["document", "range", "diagnostics"])?;
            json!({
                "operation":"code_actions",
                "document":required_value(object, "document")?,
                "range":required_value(object, "range")?,
                "diagnostics":object.get("diagnostics").cloned().unwrap_or_else(|| json!([])),
            })
        }
        _ => return Err(ToolHostDependencyError::UnsupportedTool),
    };
    let bytes = serde_json::to_vec(&operation).map_err(|_| ToolHostDependencyError::Protocol)?;
    Ok(ContentHash::digest(&bytes))
}

fn canonical_mcp_digest(
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<ContentHash, ToolHostDependencyError> {
    let object = arguments
        .as_object()
        .ok_or(ToolHostDependencyError::InvalidRequest)?;
    let expanded = match tool {
        "mcp.server.list" => {
            reject_unknown(object, &[])?;
            json!({})
        }
        "mcp.capabilities" => {
            reject_unknown(object, &["server_id"])?;
            json!({"server_id":required_value(object, "server_id")?})
        }
        "mcp.invoke" => {
            reject_unknown(object, &["server_id", "kind", "name", "arguments"])?;
            json!({
                "server_id":required_value(object, "server_id")?,
                "kind":required_value(object, "kind")?,
                "name":required_value(object, "name")?,
                "arguments":object.get("arguments").cloned().unwrap_or(Value::Null),
            })
        }
        _ => return Err(ToolHostDependencyError::UnsupportedTool),
    };
    let bytes = serde_json::to_vec(&(tool, cancellation_id, normalize_json(&expanded)))
        .map_err(|_| ToolHostDependencyError::Protocol)?;
    Ok(ContentHash::digest(&bytes))
}

fn required_value(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Value, ToolHostDependencyError> {
    object
        .get(key)
        .cloned()
        .ok_or(ToolHostDependencyError::InvalidRequest)
}

fn reject_unknown(
    object: &Map<String, Value>,
    allowed: &[&str],
) -> Result<(), ToolHostDependencyError> {
    if object.keys().all(|key| allowed.contains(&key.as_str())) {
        Ok(())
    } else {
        Err(ToolHostDependencyError::InvalidRequest)
    }
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

#[allow(clippy::too_many_lines)]
fn canonical_filesystem_digest(
    tool: &str,
    arguments: &Value,
) -> Result<ContentHash, ToolHostDependencyError> {
    let object = arguments
        .as_object()
        .ok_or(ToolHostDependencyError::InvalidRequest)?;
    let value = match tool {
        "filesystem.read" => {
            let path = string(object, "path")?;
            let projection = usize_value(object, "max_projection_bytes", 64 * 1024)?;
            let range = match (
                optional_u64(object, "line_start")?,
                optional_u64(object, "line_end")?,
                optional_u64(object, "byte_offset")?,
                optional_u64(object, "byte_length")?,
            ) {
                (None, None, None, None) => json!({"kind":"all"}),
                (Some(start), Some(end), None, None) => {
                    json!({"kind":"lines","start":start,"end":end})
                }
                (None, None, Some(offset), Some(length)) => {
                    json!({"kind":"bytes","offset":offset,"length":length})
                }
                _ => return Err(ToolHostDependencyError::InvalidRequest),
            };
            json!({"action":tool,"path":path,"range":range,"max_projection_bytes":projection})
        }
        "filesystem.list" => json!({
            "action":tool,
            "path":string(object, "path")?,
            "max_depth":usize_value(object, "max_depth", 4)?,
            "include_hidden":bool_value(object, "include_hidden", false)?,
            "honor_ignore":bool_value(object, "honor_ignore", true)?,
            "ignore_patterns":string_array(object, "ignore_patterns")?,
            "max_results":usize_value(object, "max_results", 1_000)?,
        }),
        "filesystem.glob" => json!({
            "action":tool,
            "path":string(object, "path")?,
            "patterns":required_string_array(object, "patterns")?,
            "include_hidden":bool_value(object, "include_hidden", false)?,
            "honor_ignore":bool_value(object, "honor_ignore", true)?,
            "max_results":usize_value(object, "max_results", 1_000)?,
        }),
        "filesystem.grep" => json!({
            "action":tool,
            "path":string(object, "path")?,
            "pattern":string(object, "pattern")?,
            "regex":bool_value(object, "regex", false)?,
            "case_insensitive":bool_value(object, "case_insensitive", false)?,
            "file_patterns":string_array(object, "file_patterns")?,
            "before_context":usize_value(object, "before_context", 0)?,
            "after_context":usize_value(object, "after_context", 0)?,
            "max_matches":usize_value(object, "max_matches", 1_000)?,
        }),
        "filesystem.write" => {
            let content = string(object, "content")?;
            json!({
                "action":tool,
                "path":string(object, "path")?,
                "content_hash":ContentHash::digest(content.as_bytes()).to_hex(),
                "content_bytes":content.len(),
                "mode":string(object, "mode")?,
                "expected_hash":object.get("expected_hash").cloned().unwrap_or(Value::Null),
                "overwrite":bool_value(object, "overwrite", false)?,
                "create_parents":bool_value(object, "create_parents", false)?,
            })
        }
        "filesystem.edit" => {
            let replacements = object
                .get("replacements")
                .and_then(Value::as_array)
                .ok_or(ToolHostDependencyError::InvalidRequest)?
                .iter()
                .map(|item| {
                    let item = item
                        .as_object()
                        .ok_or(ToolHostDependencyError::InvalidRequest)?;
                    Ok(json!({
                        "old":string(item, "old")?,
                        "new":string(item, "new")?,
                        "expected_occurrences":usize_value(item, "expected_occurrences", 1)?,
                    }))
                })
                .collect::<Result<Vec<_>, ToolHostDependencyError>>()?;
            json!({
                "action":tool,
                "path":string(object, "path")?,
                "replacements":replacements,
                "expected_hash":object.get("expected_hash").cloned().unwrap_or(Value::Null),
            })
        }
        "filesystem.apply_patch" => {
            let patch = string(object, "patch")?;
            let hashes: BTreeMap<String, String> = serde_json::from_value(
                object
                    .get("base_hashes")
                    .cloned()
                    .ok_or(ToolHostDependencyError::InvalidRequest)?,
            )
            .map_err(|_| ToolHostDependencyError::InvalidRequest)?;
            json!({
                "action":tool,
                "patch_hash":ContentHash::digest(patch.as_bytes()).to_hex(),
                "patch_bytes":patch.len(),
                "base_hashes":hashes,
                "create_parents":bool_value(object, "create_parents", false)?,
            })
        }
        _ => return Err(ToolHostDependencyError::UnsupportedTool),
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| ToolHostDependencyError::Protocol)?;
    Ok(ContentHash::digest(&bytes))
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

fn optional_u64(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Option<u64>, ToolHostDependencyError> {
    object
        .get(key)
        .map(|value| {
            value
                .as_u64()
                .ok_or(ToolHostDependencyError::InvalidRequest)
        })
        .transpose()
}

fn usize_value(
    object: &Map<String, Value>,
    key: &str,
    default: usize,
) -> Result<usize, ToolHostDependencyError> {
    object.get(key).map_or(Ok(default), |value| {
        value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(ToolHostDependencyError::InvalidRequest)
    })
}

fn bool_value(
    object: &Map<String, Value>,
    key: &str,
    default: bool,
) -> Result<bool, ToolHostDependencyError> {
    object.get(key).map_or(Ok(default), |value| {
        value
            .as_bool()
            .ok_or(ToolHostDependencyError::InvalidRequest)
    })
}

fn string_array(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, ToolHostDependencyError> {
    object
        .get(key)
        .map_or_else(|| Ok(Vec::new()), parse_string_array)
}

fn required_string_array(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Vec<String>, ToolHostDependencyError> {
    object
        .get(key)
        .ok_or(ToolHostDependencyError::InvalidRequest)
        .and_then(parse_string_array)
}

fn parse_string_array(value: &Value) -> Result<Vec<String>, ToolHostDependencyError> {
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

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ToolHostDependencyError {
    #[error("tool-host configuration is invalid")]
    InvalidConfiguration,
    #[error("tool request is invalid")]
    InvalidRequest,
    #[error("tool is unsupported by the selected host")]
    UnsupportedTool,
    #[error("tool host is unavailable")]
    Unavailable,
    #[error("tool-host transport failed")]
    Transport,
    #[error("tool-host protocol failed")]
    Protocol,
    #[error("tool-host response exceeded the frame limit")]
    FrameTooLarge,
    #[error("tool-host request timed out")]
    Timeout,
    #[error("tool-host authorization failed")]
    Authorization,
    #[error("tool-host clock is unavailable")]
    Clock,
    #[error("tool execution receipt storage failed")]
    ReceiptStorage,
    #[error("tool execution receipt is corrupt")]
    ReceiptCorrupt,
    #[error("tool execution receipt conflicts with the requested action")]
    ReceiptConflict,
    #[error("tool execution has no durable terminal receipt")]
    ReceiptMissing,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_digest_matches_sorted_tool_contract() {
        let left = serde_json::json!({
            "z": [{"b": 2, "a": 1}],
            "path": "."
        });
        let right = serde_json::json!({
            "path": ".",
            "z": [{"a": 1, "b": 2}]
        });
        assert_eq!(
            canonical_git_digest("git.status", &left).expect("left digest"),
            canonical_git_digest("git.status", &right).expect("right digest")
        );
        assert_ne!(
            canonical_git_digest("git.status", &left).expect("status digest"),
            canonical_git_digest("git.diff", &left).expect("diff digest")
        );
    }

    #[test]
    fn host_kind_rejects_cross_host_tool_confusion() {
        assert_eq!(
            canonical_tool_name(ToolHostKind::Filesystem, "git.status"),
            Err(ToolHostDependencyError::UnsupportedTool)
        );
        assert_eq!(
            canonical_tool_name(ToolHostKind::Git, "filesystem.read"),
            Err(ToolHostDependencyError::UnsupportedTool)
        );
    }

    #[test]
    fn web_digest_expands_defaults_and_binds_cancellation() {
        let arguments = json!({"query":"rust","count":5});
        let digest =
            canonical_web_digest("web.search", &arguments, "cancel-1").expect("web digest");
        let expanded = normalize_json(&json!({
            "query": "rust",
            "count": 5,
            "freshness": null,
            "domain_allowlist": [],
            "domain_denylist": [],
            "language": null,
            "locale": null,
            "timeout_ms": 30_000,
        }));
        let expected =
            serde_json::to_vec(&("web.search", "cancel-1", expanded)).expect("canonical bytes");
        assert_eq!(digest, ContentHash::digest(&expected));
        assert_ne!(
            digest,
            canonical_web_digest("web.search", &arguments, "cancel-2")
                .expect("second cancellation")
        );
    }

    #[test]
    fn lsp_digest_expands_defaults_and_grant_matches_host_contract() {
        let arguments = json!({"document":"src/lib.rs"});
        let digest = canonical_lsp_digest("lsp.formatting", &arguments).expect("formatting digest");
        let expected = serde_json::to_vec(&json!({
            "operation":"formatting",
            "document":"src/lib.rs",
            "tab_size":4,
            "insert_spaces":true
        }))
        .expect("canonical LSP operation");
        assert_eq!(digest, ContentHash::digest(&expected));

        let dependency = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Lsp,
            program: "fixture".into(),
            arguments: Vec::new(),
            owner: "owner".into(),
            state_root: None,
            maximum_frame_bytes: 1024,
            request_timeout: Duration::from_secs(1),
            authorization_key: [7; 32],
        })
        .expect("LSP adapter");
        let grant = dependency
            .grant(
                &DependencyToolCommand {
                    execution_id: "tool-call:call".into(),
                    receipt_only: false,
                    session_id: "018f6f83-7b80-7000-8000-000000000001".into(),
                    workspace: PathBuf::from("workspace"),
                    call_id: "call".into(),
                    tool: "lsp.formatting".into(),
                    arguments,
                    cancellation_id: "018f6f83-7b80-7000-8000-000000000002".into(),
                },
                "lsp.formatting",
                digest,
            )
            .expect("LSP grant");
        let fields: Vec<_> = grant.split('|').collect();
        assert_eq!(fields.len(), 8);
        assert_eq!(fields[0], "v1");
        assert_eq!(fields[1], "owner");
        assert_eq!(fields[3], "call");
        assert_eq!(fields[6], digest.to_hex());
        let expected_signature = blake3::keyed_hash(&[7; 32], fields[..7].join("|").as_bytes());
        assert_eq!(fields[7], expected_signature.to_hex().as_str());
    }

    #[test]
    fn mcp_digest_expands_invoke_arguments_and_binds_cancellation() {
        let arguments = json!({
            "server_id":"docs",
            "kind":"tool",
            "name":"lookup"
        });
        let digest =
            canonical_mcp_digest("mcp.invoke", &arguments, "cancel-1").expect("MCP digest");
        let expanded = normalize_json(&json!({
            "server_id":"docs",
            "kind":"tool",
            "name":"lookup",
            "arguments":null
        }));
        let bytes =
            serde_json::to_vec(&("mcp.invoke", "cancel-1", expanded)).expect("MCP canonical");
        assert_eq!(digest, ContentHash::digest(&bytes));
        assert_ne!(
            digest,
            canonical_mcp_digest("mcp.invoke", &arguments, "cancel-2")
                .expect("second cancellation")
        );
    }

    #[test]
    fn browser_digest_expands_bounds_and_binds_cancellation() {
        let digest =
            canonical_browser_digest("browser.inspect", &json!({}), "cancel-1").expect("digest");
        let expanded = normalize_json(&json!({"maximum_bytes":128 * 1024}));
        let bytes = serde_json::to_vec(&("browser.inspect", expanded, "cancel-1"))
            .expect("browser canonical");
        assert_eq!(digest, ContentHash::digest(&bytes));
        assert_ne!(
            digest,
            canonical_browser_digest("browser.inspect", &json!({}), "cancel-2")
                .expect("second cancellation")
        );
        assert_eq!(
            canonical_browser_digest(
                "browser.navigate",
                &json!({"url":"https://example.com","extra":true}),
                "cancel-1",
            ),
            Err(ToolHostDependencyError::InvalidRequest)
        );
    }
}
