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
    time::{Instant, timeout},
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
    /// Runtime plugin API version advertised during negotiation.
    pub runtime_api_version: String,
    /// Capabilities advertised during negotiation.
    pub available_capabilities: BTreeSet<String>,
    /// Kill per-session host connections idle longer than this duration.
    pub idle_shutdown: Option<Duration>,
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
    pub event_range_start: u64,
    pub event_range_end: u64,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginNodeExecutionRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub executor_id: String,
    pub node_id: String,
    pub node_kind: String,
    pub input: Value,
    pub variables: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DependencyPluginMemoryItem {
    pub reference: String,
    pub content: String,
    pub score: Option<f64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginMemoryRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub scope: String,
    pub query: String,
    pub limit: usize,
    pub entries: Vec<DependencyPluginMemoryItem>,
    pub cancellation_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyPluginContextTransformBoundary {
    BeforeMemoryRetrieval,
    AfterMemoryRetrieval,
    BeforeCompaction,
    AfterCompaction,
    BeforeProviderProjection,
    BeforeTurnCompletion,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginContextTransformRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub boundary: DependencyPluginContextTransformBoundary,
    pub payload: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginCompactionRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub source_range_start: u64,
    pub source_range_end: u64,
    pub source_range_hash: String,
    pub current_entries: Value,
    pub proposal: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginStateChangeRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub reason: Option<String>,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyPluginDecision {
    Continue(Value),
    Replace(Value),
    Reject(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyPluginMemoryResult {
    Describe {
        scopes: BTreeSet<String>,
        capabilities: BTreeSet<String>,
        bounded_bytes: u64,
    },
    Retrieve {
        items: Vec<wire::PluginMemoryItem>,
    },
    Commit {
        retained: bool,
        references: Vec<String>,
    },
    Health {
        healthy: bool,
        item_count: u64,
        retained_bytes: u64,
    },
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
pub struct DependencyPluginStatusRecord {
    pub plugin_id: String,
    pub status: String,
    pub observer_dropped: u64,
    pub pending_deliveries: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginAuditRecord {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginDeliveryRecord {
    pub delivery_id: String,
    pub plugin_id: String,
    pub handler: String,
    pub event_type: String,
    pub event_range_start: u64,
    pub event_range_end: u64,
    pub attempts: u8,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub next_retry_at_ms: i64,
    pub terminal: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginHealthRecord {
    pub loaded: usize,
    pub running: usize,
    pub observer_dropped: u64,
    pub pending_deliveries: usize,
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

    async fn execute_node(
        &self,
        request: DependencyPluginNodeExecutionRequest,
    ) -> Result<(Value, u8), PluginDependencyError>;

    async fn memory(
        &self,
        operation: String,
        request: DependencyPluginMemoryRequest,
    ) -> Result<(DependencyPluginMemoryResult, u8), PluginDependencyError>;

    async fn compaction_propose(
        &self,
        request: DependencyPluginCompactionRequest,
    ) -> Result<(Value, u64, u8), PluginDependencyError>;

    async fn context_transform(
        &self,
        request: DependencyPluginContextTransformRequest,
    ) -> Result<(Value, u8), PluginDependencyError>;

    async fn cancel(
        &self,
        session_id: String,
        invocation_id: String,
    ) -> Result<(), PluginDependencyError>;

    async fn state_change(
        &self,
        operation: &str,
        request: DependencyPluginStateChangeRequest,
    ) -> Result<DependencyPluginAuditRecord, PluginDependencyError>;

    async fn status(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<DependencyPluginStatusRecord, PluginDependencyError>;

    async fn health(
        &self,
        session_id: String,
    ) -> Result<DependencyPluginHealthRecord, PluginDependencyError>;

    async fn audits(
        &self,
        session_id: String,
    ) -> Result<Vec<DependencyPluginAuditRecord>, PluginDependencyError>;

    async fn deliveries(
        &self,
        session_id: String,
    ) -> Result<Vec<DependencyPluginDeliveryRecord>, PluginDependencyError>;

    async fn shutdown(&self);
}

struct Connection {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    last_used: Instant,
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
    /// executable, root, key, frame bound, timeout, API version, or capability
    /// is absent.
    pub fn new(config: ProcessPluginDependencyConfig) -> Result<Self, PluginDependencyError> {
        if config.program.trim().is_empty()
            || config.owner_id.trim().is_empty()
            || config.sessions_root.as_os_str().is_empty()
            || config.executable_roots.is_empty()
            || config.authorization_key == [0; 32]
            || config.maximum_frame_bytes == 0
            || config.request_timeout.is_zero()
            || config.runtime_api_version.trim().is_empty()
            || config.available_capabilities.is_empty()
        {
            return Err(PluginDependencyError::InvalidConfiguration);
        }
        let dependency = Self {
            key: Arc::new(AuthorizationKey::from_bytes(config.authorization_key)),
            config: Arc::new(config),
            connections: Arc::new(Mutex::new(BTreeMap::new())),
        };
        if let Some(idle) = dependency.config.idle_shutdown
            && !idle.is_zero()
        {
            let reaper = dependency.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(idle).await;
                    reaper.drop_idle(idle).await;
                }
            });
        }
        Ok(dependency)
    }

    #[must_use]
    pub fn derive_authorization_key(seed: &[u8]) -> [u8; 32] {
        *blake3::hash(seed).as_bytes()
    }

    async fn drop_idle(&self, idle: Duration) {
        let mut connections = self.connections.lock().await;
        let stale = connections
            .iter()
            .filter_map(|(session, connection)| {
                (connection.last_used.elapsed() >= idle).then(|| session.clone())
            })
            .collect::<Vec<_>>();
        for session in stale {
            if let Some(mut connection) = connections.remove(&session) {
                let _ = connection.child.kill().await;
                let _ = connection.child.wait().await;
            }
        }
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
        let mut connection = Connection {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            last_used: Instant::now(),
        };
        // Compatibility validation on (re)start: the fresh host must accept
        // the exact protocol and runtime API version before any operation.
        let negotiated = timeout(
            self.config.request_timeout,
            connection.exchange_raw(&wire::PluginCommand::Negotiate {
                protocol_version: wire::CURRENT_PROTOCOL_VERSION,
                runtime_api_version: self.config.runtime_api_version.clone(),
                capabilities: self.config.available_capabilities.clone(),
            }),
        )
        .await
        .map_err(|_| PluginDependencyError::Timeout)??;
        if !matches!(negotiated, wire::PluginResponse::Negotiated { .. }) {
            return Err(PluginDependencyError::Incompatible);
        }
        Ok(connection)
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
        let mut fresh = false;
        let result = {
            let mut connections = self.connections.lock().await;
            if !connections.contains_key(session_id) {
                let connection = self.start(session_id).await?;
                fresh = true;
                connections.insert(session_id.to_owned(), connection);
            }
            let connection = connections
                .get_mut(session_id)
                .ok_or(PluginDependencyError::Unavailable)?;
            connection.last_used = Instant::now();
            timeout(
                self.config.request_timeout,
                connection.exchange_raw(&command),
            )
            .await
            .map_err(|_| PluginDependencyError::Timeout)?
        };
        if result.is_err()
            && let Some(mut connection) = self.connections.lock().await.remove(session_id)
        {
            let _ = connection.child.kill().await;
            let _ = connection.child.wait().await;
        }
        if !fresh
            && matches!(
                result,
                Err(PluginDependencyError::Unavailable)
                    | Err(PluginDependencyError::InvalidResponse)
            )
        {
            // The supervised host may have shut down while idle. Restart once
            // and replay the operation; ambiguous external effects are never
            // redispatched by the caller (idempotency/evidence gates live in
            // runtime logic).
            let restarted = {
                let mut connections = self.connections.lock().await;
                if !connections.contains_key(session_id) {
                    let connection = self.start(session_id).await?;
                    connections.insert(session_id.to_owned(), connection);
                }
                let connection = connections
                    .get_mut(session_id)
                    .ok_or(PluginDependencyError::Unavailable)?;
                connection.last_used = Instant::now();
                timeout(
                    self.config.request_timeout,
                    connection.exchange_raw(&command),
                )
                .await
                .map_err(|_| PluginDependencyError::Timeout)?
            };
            if restarted.is_err()
                && let Some(mut connection) = self.connections.lock().await.remove(session_id)
            {
                let _ = connection.child.kill().await;
                let _ = connection.child.wait().await;
            }
            return restarted;
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

impl Connection {
    async fn exchange_raw(
        &mut self,
        command: &wire::PluginCommand,
    ) -> Result<wire::PluginResponse, PluginDependencyError> {
        let bytes =
            serde_json::to_vec(command).map_err(|_| PluginDependencyError::InvalidResponse)?;
        self.stdin
            .write_all(&bytes)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        self.stdin
            .flush()
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        let mut line = String::new();
        let read = self
            .stdout
            .read_line(&mut line)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        if read == 0 || line.len() > 1_048_576 {
            return Err(PluginDependencyError::InvalidResponse);
        }
        serde_json::from_str(&line).map_err(|_| PluginDependencyError::InvalidResponse)
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
            &request.event_range_start,
            &request.event_range_end,
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

    async fn execute_node(
        &self,
        request: DependencyPluginNodeExecutionRequest,
    ) -> Result<(Value, u8), PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation = (
            &request.plugin_id,
            &request.invocation_id,
            &request.executor_id,
            &request.node_id,
            &request.node_kind,
            &request.input,
            &request.variables,
            &request.readable_state,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.execute_node",
            &operation,
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::ExecuteNode {
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    executor_id: request.executor_id,
                    node_id: request.node_id,
                    node_kind: request.node_kind,
                    input: request.input,
                    variables: request.variables,
                    readable_state: request.readable_state,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::NodeResult { value, audit } => Ok((value, audit.attempts)),
            response => Err(map_failure(response)),
        }
    }

    async fn memory(
        &self,
        operation: String,
        request: DependencyPluginMemoryRequest,
    ) -> Result<(DependencyPluginMemoryResult, u8), PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation_auth = (
            &request.plugin_id,
            &request.invocation_id,
            &operation,
            &request.scope,
            &request.query,
            &request.limit,
            &request.entries,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            &format!("plugin.memory_{operation}"),
            &operation_auth,
        )?;
        let command = match operation.as_str() {
            "describe" => wire::PluginCommand::MemoryDescribe {
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                authorization,
            },
            "retrieve" => wire::PluginCommand::MemoryRetrieve {
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                scope: request.scope,
                query: request.query,
                limit: request.limit,
                authorization,
            },
            "commit_write" => wire::PluginCommand::MemoryCommitWrite {
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                scope: request.scope,
                entries: request
                    .entries
                    .into_iter()
                    .map(|item| wire::PluginMemoryItem {
                        reference: item.reference,
                        content: item.content,
                        score: item.score,
                        created_at_ms: item.created_at_ms,
                    })
                    .collect(),
                authorization,
            },
            "health" => wire::PluginCommand::MemoryHealth {
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                authorization,
            },
            _ => return Err(PluginDependencyError::InvalidRequest),
        };
        match self.exchange(&request.session_id, command).await? {
            wire::PluginResponse::MemoryDescribed {
                scopes,
                capabilities,
                bounded_bytes,
                ..
            } => Ok((
                DependencyPluginMemoryResult::Describe {
                    scopes,
                    capabilities,
                    bounded_bytes,
                },
                1,
            )),
            wire::PluginResponse::MemoryRetrieved { items, .. } => {
                Ok((DependencyPluginMemoryResult::Retrieve { items }, 1))
            }
            wire::PluginResponse::MemoryWriteCommitted {
                retained,
                references,
                ..
            } => Ok((
                DependencyPluginMemoryResult::Commit {
                    retained,
                    references,
                },
                1,
            )),
            wire::PluginResponse::MemoryHealthResult {
                healthy,
                item_count,
                retained_bytes,
                ..
            } => Ok((
                DependencyPluginMemoryResult::Health {
                    healthy,
                    item_count,
                    retained_bytes,
                },
                1,
            )),
            response => Err(map_failure(response)),
        }
    }

    async fn compaction_propose(
        &self,
        request: DependencyPluginCompactionRequest,
    ) -> Result<(Value, u64, u8), PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation = (
            &request.plugin_id,
            &request.invocation_id,
            &request.source_range_start,
            &request.source_range_end,
            &request.source_range_hash,
            &request.current_entries,
            &request.proposal,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.compaction_propose",
            &operation,
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::CompactionPropose {
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    source_range_start: request.source_range_start,
                    source_range_end: request.source_range_end,
                    source_range_hash: request.source_range_hash,
                    current_entries: request.current_entries,
                    proposal: request.proposal,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::CompactionProposalAccepted {
                replacement,
                size_bytes,
                ..
            } => Ok((replacement, size_bytes, 1)),
            response => Err(map_failure(response)),
        }
    }

    async fn context_transform(
        &self,
        request: DependencyPluginContextTransformRequest,
    ) -> Result<(Value, u8), PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation = (
            &request.plugin_id,
            &request.invocation_id,
            &request.transform_id,
            &request.boundary,
            &request.payload,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.context_transform",
            &operation,
        )?;
        let boundary = match request.boundary {
            DependencyPluginContextTransformBoundary::BeforeMemoryRetrieval => {
                wire::PluginContextTransformBoundary::BeforeMemoryRetrieval
            }
            DependencyPluginContextTransformBoundary::AfterMemoryRetrieval => {
                wire::PluginContextTransformBoundary::AfterMemoryRetrieval
            }
            DependencyPluginContextTransformBoundary::BeforeCompaction => {
                wire::PluginContextTransformBoundary::BeforeCompaction
            }
            DependencyPluginContextTransformBoundary::AfterCompaction => {
                wire::PluginContextTransformBoundary::AfterCompaction
            }
            DependencyPluginContextTransformBoundary::BeforeProviderProjection => {
                wire::PluginContextTransformBoundary::BeforeProviderProjection
            }
            DependencyPluginContextTransformBoundary::BeforeTurnCompletion => {
                wire::PluginContextTransformBoundary::BeforeTurnCompletion
            }
        };
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::ContextTransform {
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    transform_id: request.transform_id,
                    boundary,
                    payload: request.payload,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::TransformResult { value, .. } => Ok((value, 1)),
            response => Err(map_failure(response)),
        }
    }

    async fn cancel(
        &self,
        session_id: String,
        invocation_id: String,
    ) -> Result<(), PluginDependencyError> {
        match self
            .exchange(&session_id, wire::PluginCommand::Cancel { invocation_id })
            .await?
        {
            wire::PluginResponse::Cancelled { .. } => Ok(()),
            response => Err(map_failure(response)),
        }
    }

    async fn state_change(
        &self,
        operation: &str,
        request: DependencyPluginStateChangeRequest,
    ) -> Result<DependencyPluginAuditRecord, PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation_name = format!("plugin.{operation}");
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            &operation_name,
            &request.plugin_id,
        )?;
        let command = match operation {
            "disable" => wire::PluginCommand::Disable {
                plugin_id: request.plugin_id,
                authorization,
            },
            "quarantine" => wire::PluginCommand::Quarantine {
                plugin_id: request.plugin_id,
                reason_code: request.reason.unwrap_or_else(|| "quarantined".to_owned()),
                authorization,
            },
            "reload" => wire::PluginCommand::Reload {
                plugin_id: request.plugin_id,
                authorization,
            },
            "unquarantine" => wire::PluginCommand::Unquarantine {
                plugin_id: request.plugin_id,
                authorization,
            },
            _ => return Err(PluginDependencyError::InvalidRequest),
        };
        match self.exchange(&request.session_id, command).await? {
            wire::PluginResponse::StateChanged { audit, .. } => Ok(DependencyPluginAuditRecord {
                plugin_id: audit.plugin_id,
                invocation_id: audit.invocation_id,
                operation: audit.operation,
                outcome: audit.outcome,
                attempts: audit.attempts,
            }),
            response => Err(map_failure(response)),
        }
    }

    async fn status(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<DependencyPluginStatusRecord, PluginDependencyError> {
        let _ = session_id;
        let _ = plugin_id;
        Err(PluginDependencyError::Unavailable)
    }

    async fn health(
        &self,
        session_id: String,
    ) -> Result<DependencyPluginHealthRecord, PluginDependencyError> {
        match self
            .exchange(&session_id, wire::PluginCommand::Health)
            .await?
        {
            wire::PluginResponse::Health {
                loaded,
                running,
                observer_dropped,
            } => Ok(DependencyPluginHealthRecord {
                loaded,
                running,
                observer_dropped,
                pending_deliveries: 0,
            }),
            response => Err(map_failure(response)),
        }
    }

    async fn audits(
        &self,
        session_id: String,
    ) -> Result<Vec<DependencyPluginAuditRecord>, PluginDependencyError> {
        match self
            .exchange(
                &session_id,
                wire::PluginCommand::AuditList {
                    since_invocation_id: None,
                    limit: 1024,
                },
            )
            .await?
        {
            wire::PluginResponse::AuditListed { audits, .. } => Ok(audits
                .into_iter()
                .map(|audit| DependencyPluginAuditRecord {
                    plugin_id: audit.plugin_id,
                    invocation_id: audit.invocation_id,
                    operation: audit.operation,
                    outcome: audit.outcome,
                    attempts: audit.attempts,
                })
                .collect()),
            response => Err(map_failure(response)),
        }
    }

    async fn deliveries(
        &self,
        session_id: String,
    ) -> Result<Vec<DependencyPluginDeliveryRecord>, PluginDependencyError> {
        let _ = session_id;
        let _ = self;
        Ok(Vec::new())
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
    #[error("plugin host is incompatible after restart")]
    Incompatible,
    #[error("system clock is invalid")]
    Clock,
}
