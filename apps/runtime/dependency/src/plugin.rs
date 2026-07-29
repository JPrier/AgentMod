//! Supervised runtime-to-plugin-host process transport.
#![allow(
    missing_docs,
    reason = "dependency-local transport records are self-describing"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_plugin_protocol as wire;
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::timeout,
};

#[derive(Clone, Debug)]
pub struct ProcessPluginDependencyConfig {
    pub program: String,
    pub arguments: Vec<String>,
    pub owner_id: String,
    pub sessions_root: PathBuf,
    pub executable_roots: Vec<PathBuf>,
    pub authorization_key: [u8; 32],
    pub maximum_frame_bytes: usize,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginLoadRequest {
    pub session_id: String,
    pub manifest_json: String,
    pub configuration: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginInvocationRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub kind: String,
    pub payload: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginObservationRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyPluginDecision {
    Continue(Value),
    Replace(Value),
    Reject(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginLoadResult {
    pub plugin_id: String,
    pub state_version: u32,
    pub attempts: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginObservationResult {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginManifestSource {
    pub locator: String,
    pub format: String,
    pub contents: String,
}

/// Reads exact bounded plugin manifest files without interpreting them.
///
/// # Errors
///
/// Returns a classified dependency error for an unsupported extension,
/// non-file source, size/count violation, or unavailable file.
pub async fn read_plugin_manifest_sources(
    paths: &[PathBuf],
) -> Result<Vec<DependencyPluginManifestSource>, PluginDependencyError> {
    if paths.len() > 256 {
        return Err(PluginDependencyError::InvalidRequest);
    }
    let mut sources = Vec::with_capacity(paths.len());
    for path in paths {
        let metadata = fs::metadata(path)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        if !metadata.is_file() || metadata.len() > 1024 * 1024 {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let format = match path.extension().and_then(|extension| extension.to_str()) {
            Some("toml") => "toml",
            Some("json") => "json",
            _ => return Err(PluginDependencyError::InvalidRequest),
        };
        let contents = fs::read_to_string(path)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        sources.push(DependencyPluginManifestSource {
            locator: path.to_string_lossy().into_owned(),
            format: format.to_owned(),
            contents,
        });
    }
    Ok(sources)
}

#[async_trait]
pub trait RuntimePluginDependencyPort: Send + Sync {
    async fn negotiate(
        &self,
        session_id: String,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<BTreeSet<String>, PluginDependencyError>;

    async fn validate_set(
        &self,
        session_id: String,
        manifests_json: Vec<String>,
    ) -> Result<Vec<String>, PluginDependencyError>;

    async fn load(
        &self,
        request: DependencyPluginLoadRequest,
    ) -> Result<DependencyPluginLoadResult, PluginDependencyError>;

    async fn invoke(
        &self,
        request: DependencyPluginInvocationRequest,
    ) -> Result<(DependencyPluginDecision, u8), PluginDependencyError>;

    async fn observe(
        &self,
        request: DependencyPluginObservationRequest,
    ) -> Result<DependencyPluginObservationResult, PluginDependencyError>;

    async fn shutdown(&self);
}

struct Connection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Clone)]
pub struct ProcessPluginDependency {
    config: Arc<ProcessPluginDependencyConfig>,
    key: Arc<AuthorizationKey>,
    connections: Arc<Mutex<BTreeMap<String, Connection>>>,
}

impl ProcessPluginDependency {
    /// Creates a validated supervised plugin-host transport.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDependencyError::InvalidConfiguration`] when a required
    /// executable, root, key, frame bound, or timeout is absent.
    pub fn new(config: ProcessPluginDependencyConfig) -> Result<Self, PluginDependencyError> {
        if config.program.trim().is_empty()
            || config.owner_id.trim().is_empty()
            || config.sessions_root.as_os_str().is_empty()
            || config.executable_roots.is_empty()
            || config.authorization_key == [0; 32]
            || config.maximum_frame_bytes == 0
            || config.request_timeout.is_zero()
        {
            return Err(PluginDependencyError::InvalidConfiguration);
        }
        Ok(Self {
            key: Arc::new(AuthorizationKey::from_bytes(config.authorization_key)),
            config: Arc::new(config),
            connections: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    #[must_use]
    pub fn derive_authorization_key(seed: &[u8]) -> [u8; 32] {
        *blake3::hash(seed).as_bytes()
    }

    async fn start(&self, session_id: &str) -> Result<Connection, PluginDependencyError> {
        validate_id(session_id)?;
        let working_directory = self.config.sessions_root.join(session_id);
        fs::create_dir_all(&working_directory)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        let executable_roots = self
            .config
            .executable_roots
            .iter()
            .map(|root| root.to_string_lossy())
            .collect::<Vec<_>>()
            .join(";");
        let mut child = Command::new(&self.config.program);
        child
            .args(&self.config.arguments)
            .current_dir(working_directory)
            .env_clear()
            .env("AGENTMOD_PLUGIN_OWNER", &self.config.owner_id)
            .env("AGENTMOD_PLUGIN_SESSION", session_id)
            .env(
                "AGENTMOD_PLUGIN_AUTH_KEY",
                encode_hex(&self.config.authorization_key),
            )
            .env("AGENTMOD_PLUGIN_EXECUTABLE_ROOTS", executable_roots)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = child
            .spawn()
            .map_err(|_| PluginDependencyError::Unavailable)?;
        let stdin = child
            .stdin
            .take()
            .ok_or(PluginDependencyError::Unavailable)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(PluginDependencyError::Unavailable)?;
        Ok(Connection {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    async fn exchange(
        &self,
        session_id: &str,
        command: wire::PluginCommand,
    ) -> Result<wire::PluginResponse, PluginDependencyError> {
        let bytes =
            serde_json::to_vec(&command).map_err(|_| PluginDependencyError::InvalidResponse)?;
        if bytes.len() > self.config.maximum_frame_bytes {
            return Err(PluginDependencyError::FrameTooLarge);
        }
        let result = {
            let mut connections = self.connections.lock().await;
            if !connections.contains_key(session_id) {
                let connection = self.start(session_id).await?;
                connections.insert(session_id.to_owned(), connection);
            }
            let connection = connections
                .get_mut(session_id)
                .ok_or(PluginDependencyError::Unavailable)?;
            timeout(self.config.request_timeout, async {
                connection
                    .stdin
                    .write_all(&bytes)
                    .await
                    .map_err(|_| PluginDependencyError::Unavailable)?;
                connection
                    .stdin
                    .write_all(b"\n")
                    .await
                    .map_err(|_| PluginDependencyError::Unavailable)?;
                connection
                    .stdin
                    .flush()
                    .await
                    .map_err(|_| PluginDependencyError::Unavailable)?;
                let mut line = String::new();
                let read = connection
                    .stdout
                    .read_line(&mut line)
                    .await
                    .map_err(|_| PluginDependencyError::Unavailable)?;
                if read == 0 || line.len() > self.config.maximum_frame_bytes {
                    return Err(PluginDependencyError::InvalidResponse);
                }
                serde_json::from_str(&line).map_err(|_| PluginDependencyError::InvalidResponse)
            })
            .await
            .map_err(|_| PluginDependencyError::Timeout)?
        };
        if result.is_err()
            && let Some(mut connection) = self.connections.lock().await.remove(session_id)
        {
            let _ = connection.child.kill().await;
            let _ = connection.child.wait().await;
        }
        result
    }

    fn authorization<T: Serialize>(
        &self,
        session_id: &str,
        call_id: String,
        cancellation_id: String,
        action: &str,
        operation: &T,
    ) -> Result<wire::PluginAuthorization, PluginDependencyError> {
        let bytes =
            serde_json::to_vec(operation).map_err(|_| PluginDependencyError::InvalidRequest)?;
        let digest = ContentHash::digest(&bytes);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PluginDependencyError::Clock)?;
        let issued_at = i64::try_from(now.as_millis()).map_err(|_| PluginDependencyError::Clock)?;
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: self.config.owner_id.clone(),
                session: session_id.to_owned(),
                call_id: call_id.clone(),
                action: action.to_owned(),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(issued_at),
                expires_at: TimestampMillis::new(issued_at.saturating_add(30_000)),
                nonce: uuid::Uuid::now_v7().to_string(),
            },
            &self.key,
        )
        .map_err(|_| PluginDependencyError::Authorization)?;
        Ok(wire::PluginAuthorization {
            owner_id: self.config.owner_id.clone(),
            session_id: session_id.to_owned(),
            call_id,
            normalized_digest: digest.to_hex(),
            grant,
            cancellation_id,
        })
    }
}

#[async_trait]
impl RuntimePluginDependencyPort for ProcessPluginDependency {
    async fn negotiate(
        &self,
        session_id: String,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<BTreeSet<String>, PluginDependencyError> {
        match self
            .exchange(
                &session_id,
                wire::PluginCommand::Negotiate {
                    protocol_version: wire::CURRENT_PROTOCOL_VERSION,
                    runtime_api_version,
                    capabilities,
                },
            )
            .await?
        {
            wire::PluginResponse::Negotiated { capabilities, .. } => Ok(capabilities),
            response => Err(map_failure(response)),
        }
    }

    async fn validate_set(
        &self,
        session_id: String,
        manifests_json: Vec<String>,
    ) -> Result<Vec<String>, PluginDependencyError> {
        let manifests = manifests_json
            .iter()
            .map(|manifest| {
                serde_json::from_str(manifest).map_err(|_| PluginDependencyError::InvalidRequest)
            })
            .collect::<Result<Vec<wire::PluginManifest>, _>>()?;
        match self
            .exchange(&session_id, wire::PluginCommand::ValidateSet { manifests })
            .await?
        {
            wire::PluginResponse::SetValidated { plugin_ids } => Ok(plugin_ids),
            response => Err(map_failure(response)),
        }
    }

    async fn load(
        &self,
        request: DependencyPluginLoadRequest,
    ) -> Result<DependencyPluginLoadResult, PluginDependencyError> {
        let manifest: wire::PluginManifest = serde_json::from_str(&request.manifest_json)
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let call_id = uuid::Uuid::now_v7().to_string();
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.load",
            &(&manifest, &request.configuration),
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::Load {
                    manifest: Box::new(manifest),
                    configuration: request.configuration,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::Loaded {
                plugin_id,
                state_version,
                audit,
            } => Ok(DependencyPluginLoadResult {
                plugin_id,
                state_version,
                attempts: audit.attempts,
            }),
            response => Err(map_failure(response)),
        }
    }

    async fn invoke(
        &self,
        request: DependencyPluginInvocationRequest,
    ) -> Result<(DependencyPluginDecision, u8), PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation = (
            &request.plugin_id,
            &request.invocation_id,
            &request.handler,
            "intercept",
            &request.kind,
            &request.payload,
            &request.readable_state,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.intercept",
            &operation,
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::Intercept {
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    handler: request.handler,
                    proposal_type: request.kind,
                    proposal: request.payload,
                    readable_state: request.readable_state,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::Continue { proposal, audit } => {
                Ok((DependencyPluginDecision::Continue(proposal), audit.attempts))
            }
            wire::PluginResponse::Replace { proposal, audit } => {
                Ok((DependencyPluginDecision::Replace(proposal), audit.attempts))
            }
            wire::PluginResponse::Reject { reason, audit } => {
                Ok((DependencyPluginDecision::Reject(reason), audit.attempts))
            }
            response => Err(map_failure(response)),
        }
    }

    async fn observe(
        &self,
        request: DependencyPluginObservationRequest,
    ) -> Result<DependencyPluginObservationResult, PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation = (
            &request.plugin_id,
            &request.invocation_id,
            &request.handler,
            &request.event_type,
            &request.event,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.observe",
            &operation,
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::Observe {
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    handler: request.handler,
                    event_type: request.event_type,
                    event: request.event,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::Observation {
                accepted,
                queue_depth,
                dropped,
                ..
            } => Ok(DependencyPluginObservationResult {
                accepted,
                queue_depth,
                dropped,
            }),
            response => Err(map_failure(response)),
        }
    }

    async fn shutdown(&self) {
        let connections = std::mem::take(&mut *self.connections.lock().await);
        for (_, mut connection) in connections {
            let _ = connection.child.kill().await;
            let _ = connection.child.wait().await;
        }
    }
}

fn map_failure(response: wire::PluginResponse) -> PluginDependencyError {
    match response {
        wire::PluginResponse::Failed {
            code, retryable, ..
        } => PluginDependencyError::Rejected { code, retryable },
        _ => PluginDependencyError::InvalidResponse,
    }
}

fn validate_id(value: &str) -> Result<(), PluginDependencyError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        Err(PluginDependencyError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        },
    )
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginDependencyError {
    #[error("invalid plugin dependency configuration")]
    InvalidConfiguration,
    #[error("invalid plugin request")]
    InvalidRequest,
    #[error("plugin host is unavailable")]
    Unavailable,
    #[error("plugin host request timed out")]
    Timeout,
    #[error("plugin host frame is too large")]
    FrameTooLarge,
    #[error("plugin host returned an invalid response")]
    InvalidResponse,
    #[error("plugin authorization failed")]
    Authorization,
    #[error("plugin host rejected the request with `{code}`")]
    Rejected { code: String, retryable: bool },
    #[error("system clock is invalid")]
    Clock,
}
