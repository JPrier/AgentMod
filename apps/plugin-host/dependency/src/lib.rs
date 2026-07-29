//! External process, persistence, authorization, and plugin-SDK adapters.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_plugin_sdk as sdk;
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationKey, ExpectedAuthorization, verify_authorization,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, RwLock, mpsc},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Dependency-owned plugin classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyPluginClass {
    /// Blocking interceptor.
    Blocking,
    /// Observer.
    Observer,
    /// Tool.
    Tool,
    /// Other extension.
    Extension,
}

/// Dependency-owned entrypoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyEntrypoint {
    /// Executable.
    pub program: String,
    /// Fixed arguments.
    pub arguments: Vec<String>,
}

/// Dependency-owned configuration schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyConfigurationSchema {
    /// ID.
    pub id: String,
    /// Version.
    pub version: u32,
    /// Required.
    pub required: bool,
    /// Inline JSON schema.
    pub inline_json: String,
}

/// Dependency-owned manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyManifest {
    /// Schema version.
    pub schema_version: u16,
    /// Plugin ID.
    pub id: String,
    /// Plugin version.
    pub version: String,
    /// Runtime API requirement.
    pub runtime_api: String,
    /// Category.
    pub category: String,
    /// Scope.
    pub scope: String,
    /// Class.
    pub class: DependencyPluginClass,
    /// Entrypoint.
    pub entrypoint: DependencyEntrypoint,
    /// Required capabilities.
    pub required_capabilities: BTreeSet<String>,
    /// Provided capabilities.
    pub provided_capabilities: BTreeSet<String>,
    /// Events.
    pub subscribed_events: BTreeSet<String>,
    /// Read authority.
    pub read_authority: BTreeSet<String>,
    /// Proposed writes.
    pub proposed_write_authority: BTreeSet<String>,
    /// Tool permissions.
    pub tool_permissions: BTreeSet<String>,
    /// Network permissions.
    pub network_permissions: BTreeSet<String>,
    /// After constraints.
    pub after: BTreeSet<String>,
    /// Before constraints.
    pub before: BTreeSet<String>,
    /// Stage.
    pub stage: u16,
    /// Priority.
    pub priority: i32,
    /// Timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: String,
    /// Attempts.
    pub max_attempts: u8,
    /// Retry backoff.
    pub retry_backoff_ms: u64,
    /// State version.
    pub state_migration_version: u32,
    /// Config schema.
    pub configuration_schema: DependencyConfigurationSchema,
}

/// Dependency-owned authorization envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAuthorization {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Call.
    pub call_id: String,
    /// Digest.
    pub normalized_digest: String,
    /// Grant.
    pub grant: String,
    /// Cancellation.
    pub cancellation_id: String,
}

/// Load request.
#[derive(Clone, Debug)]
pub struct DependencyLoadRequest {
    /// Manifest.
    pub manifest: DependencyManifest,
    /// Configuration.
    pub configuration: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Invocation request.
#[derive(Clone, Debug)]
pub struct DependencyInvocationRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Handler/tool.
    pub handler: String,
    /// Invocation operation.
    pub operation: String,
    /// Proposal/tool payload kind.
    pub kind: String,
    /// Payload.
    pub payload: Value,
    /// Readable state.
    pub readable_state: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Observer request.
#[derive(Clone, Debug)]
pub struct DependencyObservationRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Handler.
    pub handler: String,
    /// Event type.
    pub event_type: String,
    /// Event.
    pub event: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// State-change request.
#[derive(Clone, Debug)]
pub struct DependencyStateChangeRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Reason.
    pub reason: Option<String>,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Invocation decision.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyDecision {
    /// Continue.
    Continue(Value),
    /// Replace.
    Replace(Value),
    /// Reject.
    Reject(String),
    /// Tool result.
    ToolResult(Value),
}

/// Load result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyLoadResult {
    /// Plugin ID.
    pub plugin_id: String,
    /// State version.
    pub state_version: u32,
    /// Attempts.
    pub attempts: u8,
}

/// Observer enqueue result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyObservationResult {
    /// Accepted.
    pub accepted: bool,
    /// Queue depth.
    pub queue_depth: usize,
    /// Drop count.
    pub dropped: u64,
}

/// Plugin status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginStatus {
    /// Active.
    Active,
    /// Disabled.
    Disabled,
    /// Quarantined.
    Quarantined,
}

/// Loaded plugin record.
#[derive(Clone, Debug)]
pub struct DependencyPluginRecord {
    /// Manifest.
    pub manifest: DependencyManifest,
    /// Status.
    pub status: DependencyPluginStatus,
    /// Drops.
    pub observer_dropped: u64,
}

/// Health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyHealth {
    /// Loaded.
    pub loaded: usize,
    /// Running.
    pub running: usize,
    /// Drops.
    pub observer_dropped: u64,
}

/// Audit entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAudit {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: Option<String>,
    /// Operation.
    pub operation: String,
    /// Outcome.
    pub outcome: String,
    /// Attempts.
    pub attempts: u8,
}

/// Hard dependency configuration.
#[derive(Clone, Debug)]
pub struct PluginDependencyConfig {
    /// Runtime API.
    pub runtime_api_version: String,
    /// Protocol version.
    pub protocol_version: u16,
    /// Available capabilities.
    pub available_capabilities: BTreeSet<String>,
    /// Authenticated owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Authorization key.
    pub authorization_key_hex: String,
    /// Durable state root.
    pub state_root: PathBuf,
    /// Approved executable roots.
    pub executable_roots: Vec<PathBuf>,
    /// Observer queue.
    pub observer_queue_capacity: usize,
    /// Response limit.
    pub max_response_bytes: usize,
    /// Calls per minute per plugin.
    pub rate_limit_per_minute: usize,
    /// Restart bound.
    pub max_restarts: u8,
    /// Audit ring bound.
    pub audit_capacity: usize,
}

/// Dependency interface.
#[async_trait]
pub trait PluginDependencyPort: Send + Sync {
    /// Negotiates protocol and capabilities.
    async fn negotiate(
        &self,
        protocol_version: u16,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDependencyError>;
    /// Validates a complete set.
    async fn validate_set(
        &self,
        manifests: Vec<DependencyManifest>,
    ) -> Result<Vec<String>, PluginDependencyError>;
    /// Loads and migrates.
    async fn load(
        &self,
        request: DependencyLoadRequest,
    ) -> Result<DependencyLoadResult, PluginDependencyError>;
    /// Gets a loaded record.
    async fn get(&self, plugin_id: String)
    -> Result<DependencyPluginRecord, PluginDependencyError>;
    /// Invokes a blocking handler or tool.
    async fn invoke(
        &self,
        request: DependencyInvocationRequest,
    ) -> Result<(DependencyDecision, u8), PluginDependencyError>;
    /// Enqueues an observation.
    async fn observe(
        &self,
        request: DependencyObservationRequest,
    ) -> Result<DependencyObservationResult, PluginDependencyError>;
    /// Cancels an invocation.
    async fn cancel(&self, invocation_id: String) -> Result<(), PluginDependencyError>;
    /// Disables.
    async fn disable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Quarantines.
    async fn quarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Health.
    async fn health(&self) -> DependencyHealth;
    /// Recent audit entries.
    async fn audits(&self) -> Vec<DependencyAudit>;
}

#[derive(Clone)]
struct LoadedPlugin {
    manifest: DependencyManifest,
    status: Arc<RwLock<DependencyPluginStatus>>,
    observer: Option<mpsc::Sender<ObserverWork>>,
    observer_depth: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ObserverWork {
    invocation_id: String,
    handler: String,
    event_type: String,
    event: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedState {
    version: u32,
    value: Value,
}

#[derive(Debug, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum WorkerRequest<'a> {
    Initialize {
        configuration: &'a Value,
        state_version: u32,
    },
    Migrate {
        from: u32,
        to: u32,
        state: &'a Value,
    },
    Intercept {
        handler: &'a str,
        proposal_type: &'a str,
        proposal: &'a Value,
        readable_state: &'a Value,
    },
    Observe {
        handler: &'a str,
        event_type: &'a str,
        event: &'a Value,
    },
    Tool {
        tool: &'a str,
        arguments: &'a Value,
        readable_state: &'a Value,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum WorkerResponse {
    Ready,
    State { state: Value },
    Continue { proposal: Value },
    Replace { proposal: Value },
    Reject { reason: String },
    ToolResult { value: Value },
    Observed,
}

/// Isolated implementation.
#[derive(Clone)]
pub struct IsolatedPluginDependency {
    config: Arc<PluginDependencyConfig>,
    key: Arc<AuthorizationKey>,
    plugins: Arc<Mutex<BTreeMap<String, LoadedPlugin>>>,
    invocations: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    nonces: Arc<Mutex<BTreeMap<String, i64>>>,
    rates: Arc<Mutex<BTreeMap<String, VecDeque<Instant>>>>,
    audits: Arc<Mutex<VecDeque<DependencyAudit>>>,
}

impl IsolatedPluginDependency {
    /// Constructs the dependency and loads durable replay state.
    ///
    /// # Errors
    ///
    /// Rejects incomplete security or resource configuration.
    pub async fn new(mut config: PluginDependencyConfig) -> Result<Self, PluginDependencyError> {
        if config.owner_id.is_empty()
            || config.session_id.is_empty()
            || config.authorization_key_hex.is_empty()
            || config.state_root.as_os_str().is_empty()
            || config.executable_roots.is_empty()
            || config.observer_queue_capacity == 0
            || config.max_response_bytes == 0
            || config.rate_limit_per_minute == 0
            || config.audit_capacity == 0
        {
            return Err(PluginDependencyError::InvalidConfiguration);
        }
        let key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| PluginDependencyError::InvalidConfiguration)?;
        config.authorization_key_hex.clear();
        fs::create_dir_all(&config.state_root)
            .await
            .map_err(redacted_io)?;
        let state_root = fs::canonicalize(&config.state_root)
            .await
            .map_err(redacted_io)?;
        config.state_root = state_root;
        let mut roots = Vec::with_capacity(config.executable_roots.len());
        for root in &config.executable_roots {
            roots.push(fs::canonicalize(root).await.map_err(redacted_io)?);
        }
        config.executable_roots = roots;
        let nonces = load_json::<BTreeMap<String, i64>>(&config.state_root.join("nonces.json"))
            .await?
            .unwrap_or_default();
        Ok(Self {
            config: Arc::new(config),
            key: Arc::new(key),
            plugins: Arc::new(Mutex::new(BTreeMap::new())),
            invocations: Arc::new(Mutex::new(BTreeMap::new())),
            nonces: Arc::new(Mutex::new(nonces)),
            rates: Arc::new(Mutex::new(BTreeMap::new())),
            audits: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    async fn authorize<T: Serialize>(
        &self,
        action: &str,
        operation: &T,
        authorization: &DependencyAuthorization,
    ) -> Result<(), PluginDependencyError> {
        let canonical =
            serde_json::to_vec(operation).map_err(|_| PluginDependencyError::Invalid)?;
        let digest = ContentHash::digest(&canonical);
        if authorization.owner_id != self.config.owner_id
            || authorization.session_id != self.config.session_id
            || authorization.normalized_digest != digest.to_hex()
        {
            return Err(PluginDependencyError::Authorization);
        }
        let now = now_millis()?;
        let claims = verify_authorization(
            &authorization.grant,
            &self.key,
            ExpectedAuthorization {
                owner: &self.config.owner_id,
                session: &self.config.session_id,
                call_id: &authorization.call_id,
                action,
                normalized_digest: digest,
            },
            TimestampMillis::new(now),
        )
        .map_err(|_| PluginDependencyError::Authorization)?;
        let mut nonces = self.nonces.lock().await;
        nonces.retain(|_, expiry| *expiry >= now);
        let nonce = format!("{}:{}:{}", claims.owner, claims.session, claims.nonce);
        if nonces.contains_key(&nonce) {
            return Err(PluginDependencyError::Replay);
        }
        nonces.insert(nonce, claims.expires_at.get());
        persist_json(&self.config.state_root.join("nonces.json"), &*nonces).await
    }

    async fn entry(&self, id: &str) -> Result<LoadedPlugin, PluginDependencyError> {
        self.plugins
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or(PluginDependencyError::NotLoaded)
    }

    async fn audit(&self, audit: DependencyAudit) {
        let mut entries = self.audits.lock().await;
        if entries.len() == self.config.audit_capacity {
            entries.pop_front();
        }
        entries.push_back(audit);
    }

    async fn enforce_rate(&self, plugin_id: &str) -> Result<(), PluginDependencyError> {
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(60);
        let mut rates = self.rates.lock().await;
        let entries = rates.entry(plugin_id.to_owned()).or_default();
        while entries.front().is_some_and(|entry| *entry < cutoff) {
            entries.pop_front();
        }
        if entries.len() >= self.config.rate_limit_per_minute {
            return Err(PluginDependencyError::RateLimited);
        }
        entries.push_back(now);
        Ok(())
    }

    async fn invoke_worker(
        &self,
        plugin: &LoadedPlugin,
        invocation_id: &str,
        request: &WorkerRequest<'_>,
    ) -> Result<(WorkerResponse, u8), PluginDependencyError> {
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        self.enforce_rate(&plugin.manifest.id).await?;
        let cancellation = CancellationToken::new();
        {
            let mut invocations = self.invocations.lock().await;
            if invocations
                .insert(invocation_id.to_owned(), cancellation.clone())
                .is_some()
            {
                return Err(PluginDependencyError::DuplicateInvocation);
            }
        }
        let configured_attempts = plugin.manifest.max_attempts.max(1);
        let maximum = configured_attempts.min(self.config.max_restarts.saturating_add(1).max(1));
        let mut attempt = 0_u8;
        let result = loop {
            attempt = attempt.saturating_add(1);
            let result = run_once(
                &plugin.manifest,
                request,
                cancellation.clone(),
                self.config.max_response_bytes,
            )
            .await;
            if result.is_ok() || attempt >= maximum || cancellation.is_cancelled() {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(
                plugin.manifest.retry_backoff_ms.min(5_000),
            ))
            .await;
        };
        self.invocations.lock().await.remove(invocation_id);
        result.map(|response| (response, attempt))
    }
}

#[async_trait]
#[allow(
    clippy::too_many_lines,
    reason = "the dependency trait implementation keeps each isolated operation mapping explicit"
)]
impl PluginDependencyPort for IsolatedPluginDependency {
    async fn negotiate(
        &self,
        protocol_version: u16,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDependencyError> {
        if protocol_version != self.config.protocol_version
            || runtime_api_version != self.config.runtime_api_version
        {
            return Err(PluginDependencyError::Incompatible);
        }
        Ok((
            self.config.protocol_version,
            self.config.runtime_api_version.clone(),
            capabilities
                .intersection(&self.config.available_capabilities)
                .cloned()
                .collect(),
        ))
    }

    async fn validate_set(
        &self,
        manifests: Vec<DependencyManifest>,
    ) -> Result<Vec<String>, PluginDependencyError> {
        let sdk_manifests = manifests
            .iter()
            .map(to_sdk_manifest)
            .collect::<Result<Vec<_>, _>>()?;
        let context = validation_context(&self.config);
        let validated = sdk::validate_plugin_set(&sdk_manifests, &context)
            .map_err(|report| PluginDependencyError::Validation(report.to_string()))?;
        Ok(validated
            .into_iter()
            .map(|plugin| plugin.manifest().identity.id.clone())
            .collect())
    }

    async fn load(
        &self,
        request: DependencyLoadRequest,
    ) -> Result<DependencyLoadResult, PluginDependencyError> {
        self.authorize(
            "plugin.load",
            &(&request.manifest, &request.configuration),
            &request.authorization,
        )
        .await?;
        sdk::validate_manifest(
            &to_sdk_manifest(&request.manifest)?,
            &validation_context(&self.config),
        )
        .map_err(|report| PluginDependencyError::Validation(report.to_string()))?;
        validate_configuration(
            &request.manifest.configuration_schema,
            &request.configuration,
        )?;
        validate_executable(&request.manifest.entrypoint.program, &self.config).await?;
        let state_path = state_path(&self.config.state_root, &request.manifest.id)?;
        let existing = load_json::<PersistedState>(&state_path).await?;
        let mut attempts = 1;
        let state = if let Some(existing) = existing {
            if existing.version > request.manifest.state_migration_version {
                return Err(PluginDependencyError::StateVersion);
            }
            if existing.version < request.manifest.state_migration_version {
                let temporary = LoadedPlugin {
                    manifest: request.manifest.clone(),
                    status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
                    observer: None,
                    observer_depth: Arc::new(AtomicU64::new(0)),
                    dropped: Arc::new(AtomicU64::new(0)),
                };
                let (response, used) = self
                    .invoke_worker(
                        &temporary,
                        &format!("migration-{}", request.authorization.call_id),
                        &WorkerRequest::Migrate {
                            from: existing.version,
                            to: request.manifest.state_migration_version,
                            state: &existing.value,
                        },
                    )
                    .await?;
                attempts = used;
                match response {
                    WorkerResponse::State { state } => PersistedState {
                        version: request.manifest.state_migration_version,
                        value: state,
                    },
                    _ => return Err(PluginDependencyError::MalformedResponse),
                }
            } else {
                existing
            }
        } else {
            let temporary = LoadedPlugin {
                manifest: request.manifest.clone(),
                status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
                observer: None,
                observer_depth: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
            };
            let (response, used) = self
                .invoke_worker(
                    &temporary,
                    &format!("initialize-{}", request.authorization.call_id),
                    &WorkerRequest::Initialize {
                        configuration: &request.configuration,
                        state_version: request.manifest.state_migration_version,
                    },
                )
                .await?;
            attempts = used;
            if !matches!(response, WorkerResponse::Ready) {
                return Err(PluginDependencyError::MalformedResponse);
            }
            PersistedState {
                version: request.manifest.state_migration_version,
                value: Value::Object(serde_json::Map::new()),
            }
        };
        persist_json(&state_path, &state).await?;
        let status = Arc::new(RwLock::new(DependencyPluginStatus::Active));
        let depth = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let observer = if request.manifest.class == DependencyPluginClass::Observer {
            let (sender, receiver) = mpsc::channel(self.config.observer_queue_capacity);
            tokio::spawn(observer_worker(
                request.manifest.clone(),
                receiver,
                Arc::clone(&depth),
                self.config.max_response_bytes,
            ));
            Some(sender)
        } else {
            None
        };
        self.plugins.lock().await.insert(
            request.manifest.id.clone(),
            LoadedPlugin {
                manifest: request.manifest.clone(),
                status,
                observer,
                observer_depth: depth,
                dropped,
            },
        );
        let audit = DependencyAudit {
            plugin_id: request.manifest.id.clone(),
            invocation_id: None,
            operation: "load".to_owned(),
            outcome: "loaded".to_owned(),
            attempts,
        };
        self.audit(audit).await;
        Ok(DependencyLoadResult {
            plugin_id: request.manifest.id,
            state_version: state.version,
            attempts,
        })
    }

    async fn get(
        &self,
        plugin_id: String,
    ) -> Result<DependencyPluginRecord, PluginDependencyError> {
        let plugin = self.entry(&plugin_id).await?;
        let status = *plugin.status.read().await;
        Ok(DependencyPluginRecord {
            manifest: plugin.manifest,
            status,
            observer_dropped: plugin.dropped.load(Ordering::Acquire),
        })
    }

    async fn invoke(
        &self,
        request: DependencyInvocationRequest,
    ) -> Result<(DependencyDecision, u8), PluginDependencyError> {
        self.authorize(
            &format!("plugin.{}", request.operation),
            &(
                &request.plugin_id,
                &request.invocation_id,
                &request.handler,
                &request.operation,
                &request.kind,
                &request.payload,
                &request.readable_state,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        let worker_request = if request.operation == "intercept" {
            WorkerRequest::Intercept {
                handler: &request.handler,
                proposal_type: &request.kind,
                proposal: &request.payload,
                readable_state: &request.readable_state,
            }
        } else {
            WorkerRequest::Tool {
                tool: &request.handler,
                arguments: &request.payload,
                readable_state: &request.readable_state,
            }
        };
        let (response, attempts) = self
            .invoke_worker(&plugin, &request.invocation_id, &worker_request)
            .await?;
        let decision = match response {
            WorkerResponse::Continue { proposal } => DependencyDecision::Continue(proposal),
            WorkerResponse::Replace { proposal } => DependencyDecision::Replace(proposal),
            WorkerResponse::Reject { reason } => DependencyDecision::Reject(reason),
            WorkerResponse::ToolResult { value } => DependencyDecision::ToolResult(value),
            _ => return Err(PluginDependencyError::MalformedResponse),
        };
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: Some(request.invocation_id),
            operation: request.operation,
            outcome: "completed".to_owned(),
            attempts,
        })
        .await;
        Ok((decision, attempts))
    }

    async fn observe(
        &self,
        request: DependencyObservationRequest,
    ) -> Result<DependencyObservationResult, PluginDependencyError> {
        self.authorize(
            "plugin.observe",
            &(
                &request.plugin_id,
                &request.invocation_id,
                &request.handler,
                &request.event_type,
                &request.event,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        let sender = plugin
            .observer
            .as_ref()
            .ok_or(PluginDependencyError::WrongClass)?;
        let work = ObserverWork {
            invocation_id: request.invocation_id,
            handler: request.handler,
            event_type: request.event_type,
            event: request.event,
        };
        let accepted = sender.try_send(work).is_ok();
        if accepted {
            plugin.observer_depth.fetch_add(1, Ordering::AcqRel);
        } else {
            plugin.dropped.fetch_add(1, Ordering::AcqRel);
        }
        Ok(DependencyObservationResult {
            accepted,
            queue_depth: usize::try_from(plugin.observer_depth.load(Ordering::Acquire))
                .unwrap_or(usize::MAX),
            dropped: plugin.dropped.load(Ordering::Acquire),
        })
    }

    async fn cancel(&self, invocation_id: String) -> Result<(), PluginDependencyError> {
        let token = self
            .invocations
            .lock()
            .await
            .get(&invocation_id)
            .cloned()
            .ok_or(PluginDependencyError::InvocationNotFound)?;
        token.cancel();
        Ok(())
    }

    async fn disable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize("plugin.disable", &request.plugin_id, &request.authorization)
            .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        *plugin.status.write().await = DependencyPluginStatus::Disabled;
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "disable".to_owned(),
            outcome: "disabled".to_owned(),
            attempts: 1,
        };
        self.audit(audit.clone()).await;
        Ok(audit)
    }

    async fn quarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize(
            "plugin.quarantine",
            &(&request.plugin_id, &request.reason),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        *plugin.status.write().await = DependencyPluginStatus::Quarantined;
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "quarantine".to_owned(),
            outcome: request.reason.unwrap_or_else(|| "quarantined".to_owned()),
            attempts: 1,
        };
        self.audit(audit.clone()).await;
        Ok(audit)
    }

    async fn health(&self) -> DependencyHealth {
        DependencyHealth {
            loaded: self.plugins.lock().await.len(),
            running: self.invocations.lock().await.len(),
            observer_dropped: self
                .plugins
                .lock()
                .await
                .values()
                .map(|plugin| plugin.dropped.load(Ordering::Acquire))
                .sum(),
        }
    }

    async fn audits(&self) -> Vec<DependencyAudit> {
        self.audits.lock().await.iter().cloned().collect()
    }
}

async fn observer_worker(
    manifest: DependencyManifest,
    mut receiver: mpsc::Receiver<ObserverWork>,
    depth: Arc<AtomicU64>,
    maximum: usize,
) {
    while let Some(work) = receiver.recv().await {
        depth.fetch_sub(1, Ordering::AcqRel);
        let cancellation = CancellationToken::new();
        let _ = run_once(
            &manifest,
            &WorkerRequest::Observe {
                handler: &work.handler,
                event_type: &work.event_type,
                event: &work.event,
            },
            cancellation,
            maximum,
        )
        .await;
        let _ = work.invocation_id;
    }
}

async fn run_once(
    manifest: &DependencyManifest,
    request: &WorkerRequest<'_>,
    cancellation: CancellationToken,
    maximum: usize,
) -> Result<WorkerResponse, PluginDependencyError> {
    let mut child = Command::new(&manifest.entrypoint.program);
    child
        .args(&manifest.entrypoint.arguments)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = child.spawn().map_err(redacted_io)?;
    let mut stdin = child.stdin.take().ok_or(PluginDependencyError::Process)?;
    let stdout = child.stdout.take().ok_or(PluginDependencyError::Process)?;
    let mut encoded = serde_json::to_vec(request).map_err(|_| PluginDependencyError::Invalid)?;
    encoded.push(b'\n');
    stdin.write_all(&encoded).await.map_err(redacted_io)?;
    drop(stdin);
    let reader = tokio::spawn(async move {
        let limit = u64::try_from(maximum.saturating_add(1)).unwrap_or(u64::MAX);
        let mut bytes = Vec::new();
        stdout
            .take(limit)
            .read_to_end(&mut bytes)
            .await
            .map_err(redacted_io)?;
        Ok::<Vec<u8>, PluginDependencyError>(bytes)
    });
    let deadline = Instant::now() + Duration::from_millis(manifest.timeout_ms);
    let status = tokio::select! {
        () = cancellation.cancelled() => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(PluginDependencyError::Cancelled);
        }
        result = timeout_at(deadline, child.wait()) => {
            match result {
                Ok(Ok(status)) => status,
                Ok(Err(error)) => return Err(redacted_io(error)),
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    return Err(PluginDependencyError::Timeout);
                }
            }
        }
    };
    if !status.success() {
        return Err(PluginDependencyError::Crashed);
    }
    let bytes = reader.await.map_err(|_| PluginDependencyError::Process)??;
    if bytes.len() > maximum {
        return Err(PluginDependencyError::ResponseTooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| PluginDependencyError::MalformedResponse)
}

fn validation_context(config: &PluginDependencyConfig) -> sdk::ValidationContext {
    sdk::ValidationContext {
        runtime_api_version: config.runtime_api_version.clone(),
        available_capabilities: config.available_capabilities.iter().cloned().collect(),
        maximum_timeout_ms: 300_000,
    }
}

#[allow(clippy::too_many_lines)]
fn to_sdk_manifest(
    manifest: &DependencyManifest,
) -> Result<sdk::PluginManifest, PluginDependencyError> {
    Ok(sdk::PluginManifest {
        schema_version: manifest.schema_version,
        identity: sdk::PluginIdentity {
            id: manifest.id.clone(),
            version: manifest.version.clone(),
            runtime_api: manifest.runtime_api.clone(),
        },
        category: match manifest.category.as_str() {
            "interceptor" => sdk::PluginCategory::Interceptor,
            "observer" => sdk::PluginCategory::Observer,
            "tool" => sdk::PluginCategory::Tool,
            "provider" => sdk::PluginCategory::Provider,
            "memory" => sdk::PluginCategory::Memory,
            "context_transform" => sdk::PluginCategory::ContextTransform,
            "compaction" => sdk::PluginCategory::Compaction,
            "session_style" => sdk::PluginCategory::SessionStyle,
            "graph_node" => sdk::PluginCategory::GraphNode,
            "permission_policy" => sdk::PluginCategory::PermissionPolicy,
            "scheduler" => sdk::PluginCategory::Scheduler,
            "frontend" => sdk::PluginCategory::Frontend,
            "artifact_processor" => sdk::PluginCategory::ArtifactProcessor,
            _ => return Err(PluginDependencyError::Invalid),
        },
        scope: match manifest.scope.as_str() {
            "invocation" => sdk::PluginScope::Invocation,
            "model_call" => sdk::PluginScope::ModelCall,
            "turn" => sdk::PluginScope::Turn,
            "session" => sdk::PluginScope::Session,
            "project" => sdk::PluginScope::Project,
            "user" => sdk::PluginScope::User,
            "runtime" => sdk::PluginScope::Runtime,
            _ => return Err(PluginDependencyError::Invalid),
        },
        classification: match manifest.class {
            DependencyPluginClass::Observer => sdk::PluginClassification::Observer,
            _ => sdk::PluginClassification::Blocking,
        },
        entrypoint: sdk::Entrypoint::Process {
            program: manifest.entrypoint.program.clone(),
            args: manifest.entrypoint.arguments.clone(),
        },
        trust: sdk::TrustLevel::ApprovedThirdParty,
        isolation: sdk::IsolationMode::Process,
        required_capabilities: manifest.required_capabilities.iter().cloned().collect(),
        provided_capabilities: manifest.provided_capabilities.iter().cloned().collect(),
        subscribed_events: manifest.subscribed_events.iter().cloned().collect(),
        authorities: sdk::AuthorityManifest {
            read: manifest
                .read_authority
                .iter()
                .map(|value| parse_authority(value))
                .collect::<Result<Vec<_>, _>>()?,
            proposed_write: manifest
                .proposed_write_authority
                .iter()
                .map(|value| parse_authority(value))
                .collect::<Result<Vec<_>, _>>()?,
        },
        permissions: sdk::PermissionManifest {
            tools: manifest.tool_permissions.iter().cloned().collect(),
            network: manifest.network_permissions.iter().cloned().collect(),
        },
        ordering: sdk::OrderingManifest {
            stage: manifest.stage,
            priority: manifest.priority,
            before: manifest.before.iter().cloned().collect(),
            after: manifest.after.iter().cloned().collect(),
        },
        configuration: sdk::ConfigurationSchemaMetadata {
            schema_id: manifest.configuration_schema.id.clone(),
            schema_version: manifest.configuration_schema.version,
            required: manifest.configuration_schema.required,
            source: sdk::ConfigurationSchemaSource::InlineJson {
                document: manifest.configuration_schema.inline_json.clone(),
            },
        },
        failure_policy: match manifest.failure_policy.as_str() {
            "reject" => sdk::FailurePolicy::Reject,
            "cancel" => sdk::FailurePolicy::Cancel,
            "disable" => sdk::FailurePolicy::Disable,
            "continue" => sdk::FailurePolicy::Continue,
            "retry" => sdk::FailurePolicy::Retry {
                max_attempts: manifest.max_attempts,
                backoff_ms: manifest.retry_backoff_ms,
            },
            _ => return Err(PluginDependencyError::Invalid),
        },
        timeout_ms: manifest.timeout_ms,
        state_migration_version: manifest.state_migration_version,
    })
}

fn parse_authority(value: &str) -> Result<sdk::AuthorityTarget, PluginDependencyError> {
    match value {
        "invocation_state" => Ok(sdk::AuthorityTarget::InvocationState),
        "model_call_state" => Ok(sdk::AuthorityTarget::ModelCallState),
        "turn_state" => Ok(sdk::AuthorityTarget::TurnState),
        "session_state" => Ok(sdk::AuthorityTarget::SessionState),
        "project_state" => Ok(sdk::AuthorityTarget::ProjectState),
        "user_state" => Ok(sdk::AuthorityTarget::UserState),
        "runtime_state" => Ok(sdk::AuthorityTarget::RuntimeState),
        "canonical_state" => Ok(sdk::AuthorityTarget::CanonicalState),
        "derived_index" => Ok(sdk::AuthorityTarget::DerivedIndex),
        "plugin_state" => Ok(sdk::AuthorityTarget::PluginState),
        "external_notification" => Ok(sdk::AuthorityTarget::ExternalNotification),
        _ => Err(PluginDependencyError::Invalid),
    }
}

fn validate_configuration(
    schema: &DependencyConfigurationSchema,
    configuration: &Value,
) -> Result<(), PluginDependencyError> {
    let document: Value = serde_json::from_str(&schema.inline_json)
        .map_err(|_| PluginDependencyError::Configuration)?;
    if schema.required && configuration.is_null() {
        return Err(PluginDependencyError::Configuration);
    }
    if !configuration.is_object() && !configuration.is_null() {
        return Err(PluginDependencyError::Configuration);
    }
    let object = configuration.as_object();
    if let Some(required) = document.get("required").and_then(Value::as_array) {
        for field in required.iter().filter_map(Value::as_str) {
            if object.is_none_or(|values| !values.contains_key(field)) {
                return Err(PluginDependencyError::Configuration);
            }
        }
    }
    if document.get("additionalProperties") == Some(&Value::Bool(false))
        && let (Some(properties), Some(values)) = (
            document.get("properties").and_then(Value::as_object),
            object,
        )
        && values.keys().any(|key| !properties.contains_key(key))
    {
        return Err(PluginDependencyError::Configuration);
    }
    Ok(())
}

async fn validate_executable(
    program: &str,
    config: &PluginDependencyConfig,
) -> Result<(), PluginDependencyError> {
    let path = fs::canonicalize(program)
        .await
        .map_err(|_| PluginDependencyError::Executable)?;
    if !path.is_file()
        || !config
            .executable_roots
            .iter()
            .any(|root| path.starts_with(root))
    {
        return Err(PluginDependencyError::Executable);
    }
    Ok(())
}

fn state_path(root: &Path, plugin_id: &str) -> Result<PathBuf, PluginDependencyError> {
    if plugin_id.is_empty()
        || plugin_id.len() > 128
        || !plugin_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
    {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(root.join(format!("{plugin_id}.state.json")))
}

async fn load_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, PluginDependencyError> {
    let candidates = generation_paths(path).await?;
    let selected = candidates
        .last()
        .cloned()
        .unwrap_or_else(|| path.to_path_buf());
    match fs::read(selected).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| PluginDependencyError::StateCorrupt),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(redacted_io(error)),
    }
}

async fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<(), PluginDependencyError> {
    let bytes = serde_json::to_vec(value).map_err(|_| PluginDependencyError::Invalid)?;
    let parent = path.parent().ok_or(PluginDependencyError::External)?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(PluginDependencyError::External)?;
    let committed = parent.join(format!("{file_name}.gen-{}.json", Uuid::now_v7()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&committed)
        .await
        .map_err(redacted_io)?;
    file.write_all(&bytes).await.map_err(redacted_io)?;
    file.sync_all().await.map_err(redacted_io)?;
    drop(file);
    for old in generation_paths(path).await? {
        if old != committed {
            let _ = fs::remove_file(old).await;
        }
    }
    Ok(())
}

async fn generation_paths(path: &Path) -> Result<Vec<PathBuf>, PluginDependencyError> {
    let parent = path.parent().ok_or(PluginDependencyError::External)?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(PluginDependencyError::External)?;
    let prefix = format!("{file_name}.gen-");
    let mut entries = fs::read_dir(parent).await.map_err(redacted_io)?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(redacted_io)? {
        let candidate = entry.path();
        if candidate.file_name().is_some_and(|value| {
            let value = value.to_string_lossy();
            value.starts_with(&prefix) && value.ends_with(".json")
        }) {
            paths.push(candidate);
        }
    }
    paths.sort();
    Ok(paths)
}

fn now_millis() -> Result<i64, PluginDependencyError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PluginDependencyError::Authorization)?
        .as_millis();
    i64::try_from(millis).map_err(|_| PluginDependencyError::Authorization)
}

fn redacted_io(_error: std::io::Error) -> PluginDependencyError {
    PluginDependencyError::External
}

/// Redacted dependency error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PluginDependencyError {
    /// Configuration.
    #[error("plugin dependency configuration is invalid")]
    InvalidConfiguration,
    /// Input.
    #[error("plugin request is invalid")]
    Invalid,
    /// Version.
    #[error("plugin protocol or API is incompatible")]
    Incompatible,
    /// Validation.
    #[error("plugin validation failed: {0}")]
    Validation(String),
    /// Configuration schema.
    #[error("plugin configuration is invalid")]
    Configuration,
    /// Authorization.
    #[error("plugin authorization denied")]
    Authorization,
    /// Replay.
    #[error("plugin authorization replay denied")]
    Replay,
    /// Executable.
    #[error("plugin executable is unavailable or outside approved roots")]
    Executable,
    /// Not loaded.
    #[error("plugin is not loaded")]
    NotLoaded,
    /// Inactive.
    #[error("plugin is disabled or quarantined")]
    Inactive,
    /// Wrong class.
    #[error("plugin operation is incompatible with its class")]
    WrongClass,
    /// Duplicate.
    #[error("plugin invocation ID is already active")]
    DuplicateInvocation,
    /// Missing invocation.
    #[error("plugin invocation was not found")]
    InvocationNotFound,
    /// Rate.
    #[error("plugin invocation rate exceeded")]
    RateLimited,
    /// Timeout.
    #[error("plugin invocation timed out")]
    Timeout,
    /// Cancelled.
    #[error("plugin invocation was cancelled")]
    Cancelled,
    /// Crash.
    #[error("plugin process crashed")]
    Crashed,
    /// Process.
    #[error("plugin process failed")]
    Process,
    /// Response.
    #[error("plugin response was malformed")]
    MalformedResponse,
    /// Bound.
    #[error("plugin response exceeded its bound")]
    ResponseTooLarge,
    /// State version.
    #[error("plugin state version is incompatible")]
    StateVersion,
    /// State.
    #[error("plugin state is corrupt")]
    StateCorrupt,
    /// External.
    #[error("plugin dependency operation failed")]
    External,
}
