//! Runtime endpoint mapping and lifecycle.

pub mod artifact;
pub mod continuation;
pub mod harness;
pub mod local_rpc;
pub mod mcp_oauth;
pub mod turn;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
};

use agentmod_runtime_logic::{
    GetRuntimeHealthCommand, LogicError, RuntimeHealthState, RuntimeLogicPort,
    harness_registry::{
        HarnessAvailability, HarnessDescriptor, HarnessRegistryLogicError, HarnessRegistryLogicPort,
    },
    history::{
        BranchSessionCommand, InspectSessionCommand, SessionHistoryLogicPort,
        SubscribeSessionCommand,
    },
    plugin_lifecycle::{
        ChangePluginLifecycleCommand, PluginLifecycleAction, PluginLifecycleLogicPort,
        RecoverPluginLifecyclesCommand,
    },
    registry::{
        CreateSessionCommand, ListSessionsCommand, SessionMcpSensitiveEntry,
        SessionMcpServerDeclaration, SessionMcpTransportDeclaration, SessionRegistryLogicError,
        SessionRegistryLogicPort,
    },
    scheduler::{
        FireProcessOutputCommand, FireRuntimeEventCommand, RuntimeSchedule,
        RuntimeScheduleLogicError, RuntimeScheduleLogicPort, ScheduleObservation, SchedulePayload,
        ScheduleTrigger, ScheduledExecution, UpsertScheduleCommand,
    },
    style::{
        InspectStyleCommand, ListStyleComponentsCommand, ListStylesCommand,
        SelectStyleBudgetsCommand, SelectStyleComponentsCommand, SelectStyleHarnessCommand,
        SessionStyleLogicError, SessionStyleLogicPort, StyleAvailability,
        StyleContextTransformDescriptor, StyleDecisionCapability, StyleEnvironment,
        StyleHarnessDescriptor, StyleInspection, StyleManifestFormat,
        StylePluginCompactorDescriptor, StylePluginMemoryProviderDescriptor, StyleSource,
        StyleSummary, ValidateStyleBindingCommand, ValidateStyleCommand,
    },
    tool::canonical_tool_groups,
};
use agentmod_runtime_protocol::{
    RuntimeHarnessDescriptor, RuntimeMcpServerDeclaration as WireMcpServerDeclaration,
    RuntimeMcpTransportDeclaration as WireMcpTransportDeclaration, RuntimeRequest, RuntimeResponse,
    RuntimeScheduleObservation, RuntimeSchedulePayload, RuntimeScheduleSpec,
    RuntimeScheduleTrigger, RuntimeScheduledExecution, RuntimeStyleAvailability,
    RuntimeStyleDiagnostic, RuntimeStyleInspection, RuntimeStyleManifestFormat,
    RuntimeStyleSourceKind, RuntimeStyleSummary, SessionSummary,
};
use thiserror::Error;

/// Service-owned health request after transport parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHealthRequest {
    /// Storage configuration copied from the service bootstrap context.
    pub configured_session_root: PathBuf,
}

/// Service-owned health response before wire mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHealthResponse {
    /// Endpoint-safe status.
    pub status: ServiceHealthStatus,
    /// Application version included in endpoint output.
    pub version: String,
}

/// Endpoint-safe health status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceHealthStatus {
    /// Runtime is ready.
    Ok,
    /// Runtime can respond but a required capability is unavailable.
    Degraded,
}

/// Service-owned plugin lifecycle transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePluginLifecycleAction {
    /// Disable while retaining state.
    Disable,
    /// Restore a disabled plugin.
    Enable,
    /// Quarantine after a policy or integrity finding.
    Quarantine,
    /// Release a quarantined plugin.
    Unquarantine,
}

/// Service-owned plugin lifecycle request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceChangePluginLifecycleRequest {
    /// Canonical session.
    pub session_id: agentmod_primitives::SessionId,
    /// Exact plugin ID.
    pub plugin_id: String,
    /// Requested lifecycle action.
    pub action: ServicePluginLifecycleAction,
    /// Redacted quarantine reason.
    pub reason_code: Option<String>,
    /// Management cancellation lineage.
    pub cancellation_id: agentmod_primitives::CancellationId,
}

/// Service-owned plugin lifecycle result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceChangePluginLifecycleResponse {
    /// Canonical session.
    pub session_id: agentmod_primitives::SessionId,
    /// Exact plugin ID.
    pub plugin_id: String,
    /// Exact plugin version.
    pub plugin_version: String,
    /// Terminal state.
    pub state: String,
    /// Terminal canonical sequence.
    pub committed_sequence: agentmod_primitives::Sequence,
    /// Whether replay already contained the terminal event.
    pub replayed: bool,
}

/// Startup plugin-lifecycle reconciliation summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRecoverPluginLifecyclesResponse {
    /// Sessions inspected.
    pub inspected_sessions: usize,
    /// Pending operations reconciled.
    pub reconciled_operations: usize,
}

/// Runtime service configuration, not a logic business command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeServiceConfig {
    /// Canonical sessions root selected at bootstrap.
    pub session_root: PathBuf,
    /// Build version.
    pub version: String,
    /// Explicit style registry sources and advertised runtime capabilities.
    pub styles: RuntimeStyleServiceConfig,
}

/// Service bootstrap configuration for the runtime style registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeStyleServiceConfig {
    /// Runtime style API semantic version.
    pub runtime_api_version: String,
    /// Validated activated-plugin set hash.
    pub plugin_set_hash: String,
    /// Optional user style root.
    pub user_style_root: Option<PathBuf>,
    /// Optional default project style root used outside session creation.
    pub project_style_root: Option<PathBuf>,
    /// Activated plugin style roots.
    pub plugin_style_roots: Vec<PathBuf>,
    /// Optional persistent compiled-cache root.
    pub cache_root: Option<PathBuf>,
    /// Advertised runtime capabilities.
    pub capabilities: BTreeSet<String>,
    /// Advertised tool groups.
    pub tool_groups: BTreeMap<String, BTreeSet<String>>,
    /// Advertised providers.
    pub providers: BTreeSet<String>,
    /// Activated plugins.
    pub plugins: BTreeSet<String>,
    /// Exact context-transform declarations from the activated plugin catalog.
    pub context_transforms: Vec<ServiceContextTransformDescriptor>,
    /// Exact memory-provider declarations from the activated plugin catalog.
    pub plugin_memory_providers: Vec<ServicePluginMemoryProviderDescriptor>,
    /// Exact compactor declarations from the activated plugin catalog.
    pub plugin_compactors: Vec<ServicePluginCompactorDescriptor>,
    /// Available memory providers.
    pub memory_providers: BTreeSet<String>,
    /// Available compaction strategies.
    pub compaction_strategies: BTreeSet<String>,
    /// Supported interceptor decisions.
    pub supported_decisions: BTreeSet<ServiceStyleDecisionCapability>,
    /// Resolved graph references.
    pub graph_references: BTreeMap<String, String>,
    /// Available harness descriptors used for style compatibility.
    pub harnesses: BTreeMap<String, ServiceHarnessDescriptor>,
}

/// Service bootstrap representation of one plugin context transform.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ServiceContextTransformDescriptor {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact transform identity.
    pub transform_id: String,
    /// Exact semantic version.
    pub version: String,
    /// Exact declaration hash.
    pub declaration_hash: String,
    /// Exact lifecycle boundary.
    pub lifecycle: String,
}

/// Service bootstrap representation of one plugin memory provider.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ServicePluginMemoryProviderDescriptor {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Stable provider identity.
    pub provider_id: String,
    /// Exact provider version.
    pub provider_version: String,
    /// Exact declaration hash.
    pub declaration_hash: String,
    /// Exact canonical plugin configuration hash.
    pub configuration_reference: String,
    /// Whether retrieval is declared.
    pub has_retrieve: bool,
    /// Whether consequential write is declared.
    pub has_write: bool,
}

/// Service bootstrap representation of one plugin compactor.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ServicePluginCompactorDescriptor {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Stable compactor identity.
    pub compactor_id: String,
    /// Exact compactor version.
    pub compactor_version: String,
    /// Exact declaration hash.
    pub declaration_hash: String,
    /// Exact canonical plugin configuration hash.
    pub configuration_reference: String,
}

/// Service bootstrap representation of one harness adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceHarnessDescriptor {
    /// Exact adapter version.
    pub version: String,
    /// Advertised capability IDs.
    pub capabilities: BTreeSet<String>,
    /// Whether the adapter accepts new sessions.
    pub available: bool,
}

/// Service-owned advertised interceptor decision.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ServiceStyleDecisionCapability {
    /// Continue.
    Continue,
    /// Replace.
    Replace,
    /// Reject.
    Reject,
    /// Require approval.
    RequireApproval,
    /// Defer.
    Defer,
    /// Cancel.
    Cancel,
    /// Fork.
    Fork,
}

impl RuntimeStyleServiceConfig {
    /// Builds the native runtime's explicit first-party capability registry.
    #[must_use]
    pub fn native(session_root: &std::path::Path) -> Self {
        let parent = session_root.parent().unwrap_or(session_root);
        Self {
            runtime_api_version: String::from("1.0.0"),
            plugin_set_hash: agentmod_primitives::ContentHash::digest(b"runtime.security@1")
                .to_string(),
            user_style_root: Some(parent.join("styles").join("user")),
            project_style_root: Some(PathBuf::from(".agentmod").join("styles")),
            plugin_style_roots: vec![parent.join("styles").join("plugins")],
            cache_root: Some(parent.join("style-cache")),
            capabilities: [
                "agents",
                "approval",
                "artifacts",
                "context",
                "events",
                "model",
                "scheduling",
                "tools",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            tool_groups: canonical_tool_groups(),
            providers: BTreeSet::from([String::from("deterministic-mock"), String::from("mock")]),
            plugins: BTreeSet::from([String::from("runtime.security")]),
            context_transforms: Vec::new(),
            plugin_memory_providers: Vec::new(),
            plugin_compactors: Vec::new(),
            memory_providers: ["none", "file", "sqlite-fts"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            compaction_strategies: [
                "none",
                "sliding_window",
                "summary",
                "artifact_handoff",
                "tool_output_eviction",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            supported_decisions: BTreeSet::from([
                ServiceStyleDecisionCapability::Continue,
                ServiceStyleDecisionCapability::Replace,
                ServiceStyleDecisionCapability::Reject,
                ServiceStyleDecisionCapability::RequireApproval,
                ServiceStyleDecisionCapability::Defer,
                ServiceStyleDecisionCapability::Cancel,
                ServiceStyleDecisionCapability::Fork,
            ]),
            graph_references: BTreeMap::new(),
            harnesses: BTreeMap::from([
                (
                    String::from("fixture"),
                    ServiceHarnessDescriptor {
                        version: String::from("1.0.0"),
                        capabilities: [
                            "cancellation",
                            "streaming",
                            "structured_context_replacement",
                            "structured_output",
                            "token_usage",
                            "tool_calls",
                        ]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                        available: true,
                    },
                ),
                (
                    String::from("native"),
                    ServiceHarnessDescriptor {
                        version: String::from("1.0.0"),
                        capabilities: [
                            "cancellation",
                            "cost_metadata",
                            "fine_grained_proposal_boundaries",
                            "images",
                            "multiple_tool_calls",
                            "provider_switching",
                            "streaming",
                            "structured_context_replacement",
                            "structured_output",
                            "token_usage",
                            "tool_calls",
                        ]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                        available: true,
                    },
                ),
            ]),
        }
    }

    /// Maps service-owned capability configuration into the logic environment
    /// shared by top-level and runtime-managed child session selection.
    #[must_use]
    pub fn logic_environment(&self, workspace: Option<&str>) -> StyleEnvironment {
        StyleEnvironment {
            runtime_api_version: self.runtime_api_version.clone(),
            plugin_set_hash: self.plugin_set_hash.clone(),
            user_style_root: self.user_style_root.clone(),
            project_style_root: workspace.map_or_else(
                || self.project_style_root.clone(),
                |workspace| Some(PathBuf::from(workspace).join(".agentmod").join("styles")),
            ),
            plugin_style_roots: self.plugin_style_roots.clone(),
            cache_root: self.cache_root.clone(),
            capabilities: self.capabilities.clone(),
            tool_groups: self.tool_groups.clone(),
            providers: self.providers.clone(),
            plugins: self.plugins.clone(),
            context_transforms: self
                .context_transforms
                .iter()
                .map(|transform| StyleContextTransformDescriptor {
                    plugin_id: transform.plugin_id.clone(),
                    transform_id: transform.transform_id.clone(),
                    version: transform.version.clone(),
                    declaration_hash: transform.declaration_hash.clone(),
                    lifecycle: transform.lifecycle.clone(),
                })
                .collect(),
            plugin_memory_providers: self
                .plugin_memory_providers
                .iter()
                .map(|provider| StylePluginMemoryProviderDescriptor {
                    plugin_id: provider.plugin_id.clone(),
                    plugin_version: provider.plugin_version.clone(),
                    provider_id: provider.provider_id.clone(),
                    provider_version: provider.provider_version.clone(),
                    declaration_hash: provider.declaration_hash.clone(),
                    configuration_reference: provider.configuration_reference.clone(),
                    has_retrieve: provider.has_retrieve,
                    has_write: provider.has_write,
                })
                .collect(),
            plugin_compactors: self
                .plugin_compactors
                .iter()
                .map(|compactor| StylePluginCompactorDescriptor {
                    plugin_id: compactor.plugin_id.clone(),
                    plugin_version: compactor.plugin_version.clone(),
                    compactor_id: compactor.compactor_id.clone(),
                    compactor_version: compactor.compactor_version.clone(),
                    declaration_hash: compactor.declaration_hash.clone(),
                    configuration_reference: compactor.configuration_reference.clone(),
                })
                .collect(),
            memory_providers: self.memory_providers.clone(),
            compaction_strategies: self.compaction_strategies.clone(),
            supported_decisions: self
                .supported_decisions
                .iter()
                .copied()
                .map(to_logic_style_decision)
                .collect(),
            graph_references: self.graph_references.clone(),
            harnesses: self
                .harnesses
                .iter()
                .map(|(id, descriptor)| {
                    (
                        id.clone(),
                        StyleHarnessDescriptor {
                            version: descriptor.version.clone(),
                            capabilities: descriptor.capabilities.clone(),
                            available: descriptor.available,
                        },
                    )
                })
                .collect(),
        }
    }
}

/// Endpoint-facing runtime service.
#[derive(Clone, Debug)]
pub struct RuntimeService<L> {
    logic: L,
    config: RuntimeServiceConfig,
}

impl<L> RuntimeService<L> {
    /// Creates a service with injected logic and endpoint bootstrap settings.
    #[must_use]
    pub const fn new(logic: L, config: RuntimeServiceConfig) -> Self {
        Self { logic, config }
    }
}

impl<L> RuntimeService<L>
where
    L: RuntimeLogicPort
        + SessionRegistryLogicPort
        + SessionHistoryLogicPort
        + SessionStyleLogicPort
        + HarnessRegistryLogicPort,
{
    /// Handles the currently implemented runtime wire endpoints.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for unsupported endpoints, invalid service
    /// configuration, or translated business failures.
    #[allow(
        clippy::too_many_lines,
        reason = "the service exhaustively maps each runtime wire route into service-owned records"
    )]
    pub fn handle_wire(&self, request: &RuntimeRequest) -> Result<RuntimeResponse, ServiceError> {
        match request {
            RuntimeRequest::Health => {
                let service_request = ServiceHealthRequest {
                    configured_session_root: self.config.session_root.clone(),
                };
                let service_response = self.health(service_request)?;
                Ok(RuntimeResponse::Health {
                    status: match service_response.status {
                        ServiceHealthStatus::Ok => "ok",
                        ServiceHealthStatus::Degraded => "degraded",
                    }
                    .into(),
                    version: service_response.version,
                })
            }
            RuntimeRequest::ListStyles => Ok(RuntimeResponse::Styles {
                styles: self
                    .list_styles(ServiceListStylesRequest)?
                    .styles
                    .into_iter()
                    .map(to_wire_style_summary)
                    .collect(),
            }),
            RuntimeRequest::InspectStyle { selector } => Ok(RuntimeResponse::StyleInspected {
                inspection: to_wire_style_inspection(
                    self.inspect_style(ServiceInspectStyleRequest {
                        selector: selector.clone(),
                    })?
                    .inspection,
                ),
            }),
            RuntimeRequest::ValidateStyle { manifest, format } => {
                let response = self.validate_style(ServiceValidateStyleRequest {
                    manifest: manifest.clone(),
                    format: from_wire_manifest_format(*format),
                })?;
                Ok(RuntimeResponse::StyleValidated {
                    valid: response.valid,
                    diagnostics: response
                        .diagnostics
                        .into_iter()
                        .map(to_wire_style_diagnostic)
                        .collect(),
                })
            }
            RuntimeRequest::CompileStyle { manifest, format } => {
                Ok(RuntimeResponse::StyleCompiled {
                    inspection: to_wire_style_inspection(
                        self.compile_style(ServiceValidateStyleRequest {
                            manifest: manifest.clone(),
                            format: from_wire_manifest_format(*format),
                        })?
                        .inspection,
                    ),
                })
            }
            RuntimeRequest::ListHarnesses => Ok(RuntimeResponse::Harnesses {
                harnesses: self
                    .logic
                    .list_harnesses()
                    .map_err(ServiceError::HarnessRegistry)?
                    .into_iter()
                    .map(to_wire_harness_descriptor)
                    .collect(),
            }),
            RuntimeRequest::InspectHarness { id } => Ok(RuntimeResponse::HarnessInspected {
                harness: to_wire_harness_descriptor(
                    self.logic
                        .inspect_harness(id)
                        .map_err(ServiceError::HarnessRegistry)?,
                ),
            }),
            RuntimeRequest::ListSessionComponents => {
                let catalog = self
                    .logic
                    .list_style_components(ListStyleComponentsCommand {
                        environment: self.style_environment(None),
                    })
                    .map_err(ServiceError::SessionStyle)?;
                Ok(RuntimeResponse::SessionComponents {
                    memory_providers: catalog.memory_providers,
                    compaction_strategies: catalog.compaction_strategies,
                })
            }
            RuntimeRequest::CreateSession {
                workspace,
                style,
                harness,
                memory,
                compaction,
                budgets,
            } => {
                let service_request = ServiceCreateSessionRequest {
                    workspace: workspace.clone(),
                    style: style.clone(),
                    harness: harness.clone(),
                    memory: memory.clone(),
                    compaction: compaction.clone(),
                    budgets: budgets.map(|budgets| ServiceExecutionBudgetOverrides {
                        max_iterations: budgets.max_iterations,
                        max_steps: budgets.max_steps,
                        max_tokens: budgets.max_tokens,
                        max_cost_micros: budgets.max_cost_micros,
                        max_duration_ms: budgets.max_duration_ms,
                    }),
                    mcp_servers: Vec::new(),
                };
                let created = self.create_session(service_request)?;
                Ok(RuntimeResponse::SessionCreated {
                    session_id: created.session_id,
                })
            }
            RuntimeRequest::CreateSessionWithMcp {
                workspace,
                style,
                harness,
                memory,
                compaction,
                budgets,
                mcp_servers,
            } => {
                let created = self.create_session(ServiceCreateSessionRequest {
                    workspace: workspace.clone(),
                    style: style.clone(),
                    harness: harness.clone(),
                    memory: memory.clone(),
                    compaction: compaction.clone(),
                    budgets: budgets.map(|budgets| ServiceExecutionBudgetOverrides {
                        max_iterations: budgets.max_iterations,
                        max_steps: budgets.max_steps,
                        max_tokens: budgets.max_tokens,
                        max_cost_micros: budgets.max_cost_micros,
                        max_duration_ms: budgets.max_duration_ms,
                    }),
                    mcp_servers: mcp_servers
                        .iter()
                        .cloned()
                        .map(from_wire_mcp_server)
                        .collect(),
                })?;
                Ok(RuntimeResponse::SessionCreated {
                    session_id: created.session_id,
                })
            }
            RuntimeRequest::ListSessions { limit } => {
                let listed = self.list_sessions(ServiceListSessionsRequest { limit: *limit })?;
                Ok(RuntimeResponse::Sessions {
                    sessions: listed
                        .sessions
                        .into_iter()
                        .map(|session| SessionSummary {
                            id: session.id,
                            workspace_label: session.workspace_label,
                            style: session.style,
                            sequence: session.sequence,
                            state: session.state,
                        })
                        .collect(),
                })
            }
            RuntimeRequest::InspectSession { session_id, at }
            | RuntimeRequest::ReplaySession { session_id, at } => {
                let inspected = self.inspect_session(ServiceInspectSessionRequest {
                    session_id: *session_id,
                    at: *at,
                })?;
                Ok(RuntimeResponse::SessionInspected {
                    session_id: inspected.session_id,
                    head_sequence: inspected.head_sequence,
                    inspected_sequence: inspected.inspected_sequence,
                    event_count: inspected.event_count,
                    state: inspected.state,
                })
            }
            RuntimeRequest::BranchSession {
                session_id,
                at,
                style,
            } => {
                let branched = self.branch_session(ServiceBranchSessionRequest {
                    parent_session_id: *session_id,
                    at: *at,
                    style: style.clone(),
                })?;
                Ok(RuntimeResponse::SessionBranched {
                    session_id: branched.session_id,
                    parent_session_id: branched.parent_session_id,
                    fork_sequence: branched.fork_sequence,
                    child_head_sequence: branched.child_head_sequence,
                })
            }
            _ => Err(ServiceError::UnsupportedEndpoint),
        }
    }

    /// Lists the live style registry.
    ///
    /// # Errors
    ///
    /// Returns a translated style-catalog error when discovery fails.
    pub fn list_styles(
        &self,
        _request: ServiceListStylesRequest,
    ) -> Result<ServiceListStylesResponse, ServiceError> {
        self.logic
            .list_styles(ListStylesCommand {
                environment: self.style_environment(None),
            })
            .map(|styles| ServiceListStylesResponse {
                styles: styles.into_iter().map(from_logic_style_summary).collect(),
            })
            .map_err(ServiceError::SessionStyle)
    }

    /// Inspects one registry entry selected by ID or exact ID/version.
    ///
    /// # Errors
    ///
    /// Returns an endpoint validation or translated style-selection error.
    pub fn inspect_style(
        &self,
        request: ServiceInspectStyleRequest,
    ) -> Result<ServiceInspectStyleResponse, ServiceError> {
        if request.selector.trim().is_empty() {
            return Err(ServiceError::InvalidStyleRequest);
        }
        self.logic
            .inspect_style(InspectStyleCommand {
                selector: request.selector,
                environment: self.style_environment(None),
            })
            .map(from_logic_style_inspection)
            .map(|inspection| ServiceInspectStyleResponse { inspection })
            .map_err(ServiceError::SessionStyle)
    }

    /// Validates one transient manifest with structured diagnostics.
    ///
    /// # Errors
    ///
    /// Returns an endpoint or style-compilation error when validation cannot run.
    pub fn validate_style(
        &self,
        request: ServiceValidateStyleRequest,
    ) -> Result<ServiceValidateStyleResponse, ServiceError> {
        let inspection = self.validate_or_compile_style(request)?;
        Ok(ServiceValidateStyleResponse {
            valid: inspection.summary.availability == ServiceStyleAvailability::Available,
            diagnostics: inspection.diagnostics,
        })
    }

    /// Compiles one transient manifest for inspection.
    ///
    /// # Errors
    ///
    /// Returns an endpoint or style-compilation error when compilation cannot run.
    pub fn compile_style(
        &self,
        request: ServiceValidateStyleRequest,
    ) -> Result<ServiceCompileStyleResponse, ServiceError> {
        self.validate_or_compile_style(request)
            .map(|inspection| ServiceCompileStyleResponse { inspection })
    }

    fn validate_or_compile_style(
        &self,
        request: ServiceValidateStyleRequest,
    ) -> Result<ServiceStyleInspection, ServiceError> {
        if request.manifest.is_empty() {
            return Err(ServiceError::InvalidStyleRequest);
        }
        self.logic
            .validate_style(ValidateStyleCommand {
                manifest: request.manifest,
                format: match request.format {
                    ServiceStyleManifestFormat::Toml => StyleManifestFormat::Toml,
                    ServiceStyleManifestFormat::Json => StyleManifestFormat::Json,
                },
                environment: self.style_environment(None),
            })
            .map(from_logic_style_inspection)
            .map_err(ServiceError::SessionStyle)
    }

    fn style_environment(&self, workspace: Option<&str>) -> StyleEnvironment {
        self.config.styles.logic_environment(workspace)
    }

    /// Validates the immutable style binding reconstructed from a session's
    /// canonical history against the current runtime catalog.
    ///
    /// This is intentionally lazy so dormant sessions do not acquire tasks,
    /// processes, or loaded transcripts merely because the daemon restarted.
    ///
    /// # Errors
    ///
    /// Returns a migration-required error for legacy unbound sessions and a
    /// precise style error when the exact persisted style is unavailable or
    /// incompatible. No replacement style is selected automatically.
    pub fn validate_session_style_compatibility(
        &self,
        session_id: agentmod_primitives::SessionId,
    ) -> Result<(), ServiceError> {
        let result = self
            .logic
            .inspect_session(InspectSessionCommand {
                sessions_root: self.config.session_root.clone(),
                session_id,
                at: None,
            })
            .map_err(|error| ServiceError::SessionHistory(error.to_string()))?;
        let binding = result
            .state
            .style_binding
            .ok_or(ServiceError::StyleMigrationRequired)?;
        self.logic
            .validate_style_binding(ValidateStyleBindingCommand {
                binding,
                environment: self.style_environment(Some(&result.state.workspace)),
            })
            .map_err(ServiceError::SessionStyle)
    }

    /// Purely reconstructs endpoint-safe structured state.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when history replay or endpoint serialization fails.
    pub fn inspect_session(
        &self,
        request: ServiceInspectSessionRequest,
    ) -> Result<ServiceInspectSessionResponse, ServiceError> {
        let result = self
            .logic
            .inspect_session(InspectSessionCommand {
                sessions_root: self.config.session_root.clone(),
                session_id: request.session_id,
                at: request.at,
            })
            .map_err(|error| ServiceError::SessionHistory(error.to_string()))?;
        let compatibility = match result.state.style_binding.clone() {
            Some(binding) => self
                .logic
                .validate_style_binding(ValidateStyleBindingCommand {
                    binding,
                    environment: self.style_environment(Some(&result.state.workspace)),
                })
                .map_or_else(
                    |error| {
                        serde_json::json!({
                            "status": "incompatible",
                            "reason": error.to_string(),
                        })
                    },
                    |()| serde_json::json!({ "status": "compatible" }),
                ),
            None => serde_json::json!({
                "status": "migration_required",
                "reason": "the session predates immutable session-style bindings",
            }),
        };
        let mut state =
            serde_json::to_value(&result.state).map_err(|_| ServiceError::StateSerialization)?;
        state
            .as_object_mut()
            .ok_or(ServiceError::StateSerialization)?
            .insert(String::from("style_compatibility"), compatibility);
        if let Some(introspection) =
            agentmod_runtime_logic::introspection::inspect_style_execution(&result.state)
                .map_err(|_| ServiceError::StateSerialization)?
        {
            state
                .as_object_mut()
                .ok_or(ServiceError::StateSerialization)?
                .insert(String::from("style_introspection"), introspection.value);
        }
        Ok(ServiceInspectSessionResponse {
            session_id: result.state.id,
            head_sequence: result.head_sequence,
            inspected_sequence: result.inspected_sequence,
            event_count: result.event_count,
            state,
        })
    }

    /// Reads one bounded verified event page for a reconnecting frontend.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when the cursor, bound, session, or journal is invalid.
    pub fn subscribe_session(
        &self,
        request: ServiceSubscribeSessionRequest,
    ) -> Result<ServiceSessionEventPage, ServiceError> {
        let result = self
            .logic
            .subscribe_session(SubscribeSessionCommand {
                sessions_root: self.config.session_root.clone(),
                session_id: request.session_id,
                after: request.after,
                limit: request.limit,
            })
            .map_err(|error| ServiceError::SessionHistory(error.to_string()))?;
        Ok(ServiceSessionEventPage {
            head_sequence: result.head_sequence,
            last_delivered_sequence: result.last_delivered_sequence,
            has_more: result.has_more,
            events: result
                .events
                .into_iter()
                .map(|event| ServiceSessionEvent {
                    event_id: event.event_id,
                    sequence: event.sequence,
                    event_type: event.event_type,
                    payload: event.payload,
                })
                .collect(),
        })
    }

    /// Creates an atomic replay-derived branch.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when validation, replay, or atomic persistence fails.
    pub fn branch_session(
        &self,
        request: ServiceBranchSessionRequest,
    ) -> Result<ServiceBranchSessionResponse, ServiceError> {
        let ServiceBranchSessionRequest {
            parent_session_id,
            at,
            style,
        } = request;
        let style_binding = style
            .as_ref()
            .map(|selector| {
                self.logic
                    .resolve_style(InspectStyleCommand {
                        selector: selector.clone(),
                        environment: self.style_environment(None),
                    })
                    .map(|resolved| resolved.binding)
                    .map_err(ServiceError::SessionStyle)
            })
            .transpose()?;
        let result = self
            .logic
            .branch_session(BranchSessionCommand {
                sessions_root: self.config.session_root.clone(),
                parent_session_id,
                at,
                style_binding,
            })
            .map_err(|error| ServiceError::SessionHistory(error.to_string()))?;
        Ok(ServiceBranchSessionResponse {
            session_id: result.session_id,
            parent_session_id: result.parent_session_id,
            fork_sequence: result.fork_sequence,
            child_head_sequence: result.child_head_sequence,
        })
    }

    /// Creates a session through service-owned request and result types.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for endpoint validation or translated business failures.
    pub fn create_session(
        &self,
        request: ServiceCreateSessionRequest,
    ) -> Result<ServiceCreateSessionResponse, ServiceError> {
        if request.workspace.trim().is_empty() || request.style.trim().is_empty() {
            return Err(ServiceError::InvalidSessionRequest);
        }
        let mut resolved = self
            .logic
            .resolve_style(InspectStyleCommand {
                selector: request.style,
                environment: self.style_environment(Some(&request.workspace)),
            })
            .map_err(ServiceError::SessionStyle)?;
        if request.memory.is_some() || request.compaction.is_some() {
            resolved = self
                .logic
                .select_style_components(SelectStyleComponentsCommand {
                    binding: resolved.binding,
                    memory: request.memory,
                    compaction: request.compaction,
                    environment: self.style_environment(Some(&request.workspace)),
                })
                .map_err(ServiceError::SessionStyle)?;
        }
        if let Some(budgets) = request.budgets {
            resolved = self
                .logic
                .select_style_budgets(SelectStyleBudgetsCommand {
                    binding: resolved.binding,
                    max_iterations: budgets.max_iterations,
                    max_steps: budgets.max_steps,
                    max_tokens: budgets.max_tokens,
                    max_cost_micros: budgets.max_cost_micros,
                    max_duration_ms: budgets.max_duration_ms,
                    environment: self.style_environment(Some(&request.workspace)),
                })
                .map_err(ServiceError::SessionStyle)?;
        }
        if let Some(harness) = request.harness {
            resolved = self
                .logic
                .select_style_harness(SelectStyleHarnessCommand {
                    binding: resolved.binding,
                    harness,
                    environment: self.style_environment(Some(&request.workspace)),
                })
                .map_err(ServiceError::SessionStyle)?;
        }
        let result = self
            .logic
            .create_session(CreateSessionCommand {
                sessions_root: self.config.session_root.clone(),
                workspace: PathBuf::from(request.workspace),
                style_binding: resolved.binding,
                mcp_servers: request
                    .mcp_servers
                    .into_iter()
                    .map(to_logic_mcp_server)
                    .collect(),
            })
            .map_err(ServiceError::SessionRegistry)?;
        Ok(ServiceCreateSessionResponse {
            session_id: result.session_id,
        })
    }

    /// Lists lightweight dormant-session metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for an invalid bound or translated business failure.
    pub fn list_sessions(
        &self,
        request: ServiceListSessionsRequest,
    ) -> Result<ServiceListSessionsResponse, ServiceError> {
        let limit =
            usize::try_from(request.limit).map_err(|_| ServiceError::InvalidSessionListLimit)?;
        let sessions = self
            .logic
            .list_sessions(ListSessionsCommand {
                sessions_root: self.config.session_root.clone(),
                limit,
            })
            .map_err(ServiceError::SessionRegistry)?
            .into_iter()
            .map(|record| ServiceSessionSummary {
                id: record.id,
                workspace_label: record.workspace_label,
                style: record.style,
                sequence: record.sequence,
                state: record.state,
            })
            .collect();
        Ok(ServiceListSessionsResponse { sessions })
    }

    /// Executes the service-owned health endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::InvalidSessionRoot`] for invalid service input or
    /// [`ServiceError::Logic`] for a translated business failure.
    pub fn health(
        &self,
        request: ServiceHealthRequest,
    ) -> Result<ServiceHealthResponse, ServiceError> {
        if request.configured_session_root.as_os_str().is_empty() {
            return Err(ServiceError::InvalidSessionRoot);
        }
        let command = GetRuntimeHealthCommand {
            canonical_session_root: request.configured_session_root,
        };
        let result = self
            .logic
            .get_health(command)
            .map_err(ServiceError::Logic)?;
        Ok(ServiceHealthResponse {
            status: match result.state {
                RuntimeHealthState::Ready => ServiceHealthStatus::Ok,
                RuntimeHealthState::Degraded => ServiceHealthStatus::Degraded,
            },
            version: self.config.version.clone(),
        })
    }
}

impl<L: PluginLifecycleLogicPort> RuntimeService<L> {
    /// Reconciles canonical pending plugin lifecycle operations during startup.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::PluginLifecycle`] when canonical replay or exact
    /// host-receipt reconciliation fails.
    pub async fn recover_pending_plugin_lifecycles(
        &self,
        limit: usize,
    ) -> Result<ServiceRecoverPluginLifecyclesResponse, ServiceError> {
        self.logic
            .recover_pending_plugin_lifecycles(RecoverPluginLifecyclesCommand {
                sessions_root: self.config.session_root.clone(),
                limit,
            })
            .await
            .map(|result| ServiceRecoverPluginLifecyclesResponse {
                inspected_sessions: result.inspected_sessions,
                reconciled_operations: result.reconciled_operations,
            })
            .map_err(|error| ServiceError::PluginLifecycle(error.to_string()))
    }

    /// Changes one exact session plugin lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::PluginLifecycle`] for invalid, conflicting, or
    /// unavailable lifecycle transitions.
    pub async fn change_plugin_lifecycle(
        &self,
        request: ServiceChangePluginLifecycleRequest,
    ) -> Result<ServiceChangePluginLifecycleResponse, ServiceError> {
        let result = self
            .logic
            .change_plugin_lifecycle(ChangePluginLifecycleCommand {
                sessions_root: self.config.session_root.clone(),
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                action: match request.action {
                    ServicePluginLifecycleAction::Disable => PluginLifecycleAction::Disable,
                    ServicePluginLifecycleAction::Enable => PluginLifecycleAction::Enable,
                    ServicePluginLifecycleAction::Quarantine => PluginLifecycleAction::Quarantine,
                    ServicePluginLifecycleAction::Unquarantine => {
                        PluginLifecycleAction::Unquarantine
                    }
                },
                reason_code: request.reason_code,
                cancellation_id: request.cancellation_id.to_string(),
            })
            .await
            .map_err(|error| ServiceError::PluginLifecycle(error.to_string()))?;
        Ok(ServiceChangePluginLifecycleResponse {
            session_id: request.session_id,
            plugin_id: result.plugin_id,
            plugin_version: result.plugin_version,
            state: result.state,
            committed_sequence: result.committed_sequence,
            replayed: result.replayed,
        })
    }
}

impl<L: RuntimeScheduleLogicPort> RuntimeService<L> {
    /// Maps runtime schedule endpoints through service-owned and logic-owned types.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] for invalid business requests or scheduler failures.
    pub fn handle_schedule_wire(
        &self,
        request: &RuntimeRequest,
    ) -> Result<RuntimeResponse, ServiceError> {
        match request {
            RuntimeRequest::UpsertSchedule { schedule } => {
                let result = self.upsert_schedule(from_wire_schedule((**schedule).clone()))?;
                Ok(RuntimeResponse::ScheduleStored {
                    schedule_id: result.schedule_id,
                    replayed: result.replayed,
                })
            }
            RuntimeRequest::RemoveSchedule { schedule_id } => {
                let existed = self
                    .logic
                    .remove_schedule(schedule_id)
                    .map_err(ServiceError::Schedule)?;
                Ok(RuntimeResponse::ScheduleRemoved { existed })
            }
            RuntimeRequest::ListSchedules { limit } => {
                let schedules = self
                    .logic
                    .list_schedules(*limit)
                    .map_err(ServiceError::Schedule)?
                    .into_iter()
                    .map(from_logic_schedule)
                    .map(to_wire_schedule)
                    .collect();
                Ok(RuntimeResponse::Schedules { schedules })
            }
            RuntimeRequest::ClaimDueSchedules { limit } => {
                let executions = self
                    .logic
                    .claim_due_schedules(*limit)
                    .map_err(ServiceError::Schedule)?
                    .into_iter()
                    .map(|execution| RuntimeScheduledExecution {
                        execution_id: execution.execution_id,
                        scheduled_for_ms: execution.scheduled_for_ms,
                        claimed_at_ms: execution.claimed_at_ms,
                        observation: execution.observation.map(to_wire_observation),
                        schedule: to_wire_schedule(from_logic_schedule(execution.schedule)),
                    })
                    .collect();
                Ok(RuntimeResponse::ScheduledExecutions { executions })
            }
            RuntimeRequest::ListPendingScheduledExecutions { limit } => {
                let executions = self
                    .logic
                    .list_pending_executions(*limit)
                    .map_err(ServiceError::Schedule)?
                    .into_iter()
                    .map(|execution| RuntimeScheduledExecution {
                        execution_id: execution.execution_id,
                        scheduled_for_ms: execution.scheduled_for_ms,
                        claimed_at_ms: execution.claimed_at_ms,
                        observation: execution.observation.map(to_wire_observation),
                        schedule: to_wire_schedule(from_logic_schedule(execution.schedule)),
                    })
                    .collect();
                Ok(RuntimeResponse::ScheduledExecutions { executions })
            }
            RuntimeRequest::CompleteScheduledExecution {
                execution_id,
                succeeded,
            } => {
                let changed = self
                    .logic
                    .complete_scheduled_execution(execution_id, *succeeded)
                    .map_err(ServiceError::Schedule)?;
                Ok(RuntimeResponse::ScheduledExecutionCompleted { changed })
            }
            _ => Err(ServiceError::UnsupportedEndpoint),
        }
    }

    fn upsert_schedule(
        &self,
        schedule: ServiceSchedule,
    ) -> Result<ServiceScheduleStoreResult, ServiceError> {
        let result = self
            .logic
            .upsert_schedule(UpsertScheduleCommand {
                schedule_id: schedule.schedule_id,
                session_id: schedule.session_id,
                idempotency_id: schedule.idempotency_id,
                style: schedule.style,
                workspace: schedule.workspace,
                permission_policy: schedule.permission_policy,
                provider: schedule.provider,
                model: schedule.model,
                token_budget: schedule.token_budget,
                cost_budget_micros: schedule.cost_budget_micros,
                trigger: to_logic_trigger(schedule.trigger),
                payload: to_logic_payload(schedule.payload),
                active: schedule.active,
            })
            .map_err(ServiceError::Schedule)?;
        Ok(ServiceScheduleStoreResult {
            schedule_id: result.schedule_id,
            replayed: result.replayed,
        })
    }

    fn fire_runtime_event(
        &self,
        source_session_id: agentmod_primitives::SessionId,
        event_id: String,
        event_type: String,
    ) -> Result<Vec<ServiceScheduledExecution>, ServiceError> {
        self.logic
            .fire_runtime_event(FireRuntimeEventCommand {
                source_session_id,
                event_id,
                event_type,
            })
            .map(|values| values.into_iter().map(from_logic_execution).collect())
            .map_err(ServiceError::Schedule)
    }

    fn fire_process_output(
        &self,
        source_session_id: agentmod_primitives::SessionId,
        output_id: String,
        process_id: String,
        output: String,
    ) -> Result<Vec<ServiceScheduledExecution>, ServiceError> {
        self.logic
            .fire_process_output(FireProcessOutputCommand {
                source_session_id,
                output_id,
                process_id,
                output,
            })
            .map(|values| values.into_iter().map(from_logic_execution).collect())
            .map_err(ServiceError::Schedule)
    }
}

/// Service-owned style list request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceListStylesRequest;

/// Service-owned style list response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceListStylesResponse {
    /// Bounded catalog rows.
    pub styles: Vec<ServiceStyleSummary>,
}

/// Service-owned style inspection request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceInspectStyleRequest {
    /// ID or exact `id@version`.
    pub selector: String,
}

/// Service-owned style inspection response.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceInspectStyleResponse {
    /// Complete inspection.
    pub inspection: ServiceStyleInspection,
}

/// Service-owned manifest format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStyleManifestFormat {
    /// TOML.
    Toml,
    /// JSON.
    Json,
}

/// Service-owned transient validation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceValidateStyleRequest {
    /// Complete source.
    pub manifest: String,
    /// Encoding.
    pub format: ServiceStyleManifestFormat,
}

/// Service-owned validation response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceValidateStyleResponse {
    /// Whether the manifest compiled in the current environment.
    pub valid: bool,
    /// Structured diagnostics.
    pub diagnostics: Vec<ServiceStyleDiagnostic>,
}

/// Service-owned compilation response.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceCompileStyleResponse {
    /// Complete compiled inspection.
    pub inspection: ServiceStyleInspection,
}

/// Service-owned style source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStyleSource {
    /// Built in.
    BuiltIn,
    /// User file.
    User,
    /// Project file.
    Project,
    /// Plugin package.
    Plugin,
    /// Inline.
    Inline,
}

/// Service-owned style availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStyleAvailability {
    /// Available.
    Available,
    /// Disabled.
    Disabled,
    /// Invalid.
    Invalid,
    /// Incompatible.
    Incompatible,
    /// Conflicting exact identity.
    Conflict,
}

/// Service-owned style diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceStyleDiagnostic {
    /// Stable code.
    pub code: String,
    /// Manifest path.
    pub path: String,
    /// Safe explanation.
    pub message: String,
    /// Remediation.
    pub help: String,
}

/// Service-owned style summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceStyleSummary {
    /// Stable ID.
    pub id: String,
    /// Semantic version.
    pub version: String,
    /// Source.
    pub source: ServiceStyleSource,
    /// Availability.
    pub availability: ServiceStyleAvailability,
    /// Manifest content hash.
    pub style_content_hash: String,
    /// Compiled cache key.
    pub compiled_cache_key: String,
    /// Required runtime capabilities.
    pub required_capabilities: Vec<String>,
}

/// Service-owned complete style inspection.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceStyleInspection {
    /// Summary.
    pub summary: ServiceStyleSummary,
    /// Safe source locator.
    pub source_locator: String,
    /// Parsed manifest.
    pub manifest: serde_json::Value,
    /// Compiled descriptor.
    pub compiled: Option<serde_json::Value>,
    /// Diagnostics.
    pub diagnostics: Vec<ServiceStyleDiagnostic>,
}

/// Service-owned create-session request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCreateSessionRequest {
    /// Endpoint workspace text.
    pub workspace: String,
    /// Endpoint style text.
    pub style: String,
    /// Optional per-session harness override.
    pub harness: Option<String>,
    /// Optional per-session memory-provider override.
    pub memory: Option<String>,
    /// Optional per-session compaction-strategy override.
    pub compaction: Option<String>,
    /// Optional per-session hard execution-budget overrides.
    pub budgets: Option<ServiceExecutionBudgetOverrides>,
    /// Exact ordered per-session MCP declarations.
    pub mcp_servers: Vec<ServiceMcpServerDeclaration>,
}

/// Service-owned MCP server declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceMcpServerDeclaration {
    /// ACP display name.
    pub name: String,
    /// Exact requested transport.
    pub transport: ServiceMcpTransportDeclaration,
}

/// Service-owned MCP transport declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceMcpTransportDeclaration {
    /// Child process speaking MCP over stdio.
    Stdio {
        /// Absolute executable path.
        program: String,
        /// Exact argument vector.
        arguments: Vec<String>,
        /// Exact environment values.
        environment: Vec<ServiceMcpSensitiveEntry>,
    },
    /// Streamable HTTP or legacy SSE endpoint.
    StreamableHttp {
        /// Secure or loopback endpoint.
        url: String,
        /// Whether ACP declared legacy SSE.
        legacy_sse: bool,
        /// Exact HTTP headers.
        headers: Vec<ServiceMcpSensitiveEntry>,
    },
}

/// Service-owned sensitive MCP entry with redacted diagnostics.
#[derive(Clone, Eq, PartialEq)]
pub struct ServiceMcpSensitiveEntry {
    /// Environment variable or HTTP header name.
    pub name: String,
    /// Exact transient value.
    pub value: String,
}

impl fmt::Debug for ServiceMcpSensitiveEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceMcpSensitiveEntry")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Service-owned optional hard execution-budget overrides.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServiceExecutionBudgetOverrides {
    /// Maximum loop/research iterations.
    pub max_iterations: Option<u32>,
    /// Maximum graph transitions.
    pub max_steps: Option<u64>,
    /// Maximum provider tokens.
    pub max_tokens: Option<u64>,
    /// Maximum cost in configured currency micros.
    pub max_cost_micros: Option<u64>,
    /// Maximum wall-clock duration.
    pub max_duration_ms: Option<u64>,
}

/// Service-owned create-session response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCreateSessionResponse {
    /// Canonical identifier.
    pub session_id: agentmod_primitives::SessionId,
}

/// Service-owned list request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceListSessionsRequest {
    /// Caller-requested bound.
    pub limit: u32,
}

/// Service-owned summary record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceSessionSummary {
    /// Session ID.
    pub id: agentmod_primitives::SessionId,
    /// Safe workspace label.
    pub workspace_label: String,
    /// Explicit style.
    pub style: String,
    /// Last known sequence.
    pub sequence: agentmod_primitives::Sequence,
    /// Lifecycle label.
    pub state: String,
}

/// Service-owned list response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceListSessionsResponse {
    /// Bounded summaries.
    pub sessions: Vec<ServiceSessionSummary>,
}

/// Service-owned point-in-time request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceInspectSessionRequest {
    /// Selected session.
    pub session_id: agentmod_primitives::SessionId,
    /// Inclusive target.
    pub at: Option<agentmod_primitives::Sequence>,
}

/// Service-owned point-in-time response.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceInspectSessionResponse {
    /// Selected session.
    pub session_id: agentmod_primitives::SessionId,
    /// Verified head.
    pub head_sequence: agentmod_primitives::Sequence,
    /// Replayed point.
    pub inspected_sequence: agentmod_primitives::Sequence,
    /// Events reduced.
    pub event_count: u64,
    /// Endpoint-safe structured state.
    pub state: serde_json::Value,
}

/// Service-owned reconnect cursor request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceSubscribeSessionRequest {
    /// Selected session.
    pub session_id: agentmod_primitives::SessionId,
    /// Last contiguous event already received.
    pub after: Option<agentmod_primitives::Sequence>,
    /// Maximum page size.
    pub limit: u32,
}

/// One service-owned canonical event projection.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceSessionEvent {
    /// Canonical event identity.
    pub event_id: agentmod_primitives::EventId,
    /// Canonical sequence.
    pub sequence: agentmod_primitives::Sequence,
    /// Stable event type.
    pub event_type: String,
    /// Typed payload.
    pub payload: serde_json::Value,
}

/// Service-owned bounded reconnect page.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceSessionEventPage {
    /// Verified journal head.
    pub head_sequence: agentmod_primitives::Sequence,
    /// Last sequence in the page.
    pub last_delivered_sequence: Option<agentmod_primitives::Sequence>,
    /// Whether an immediate next page exists.
    pub has_more: bool,
    /// Ordered events.
    pub events: Vec<ServiceSessionEvent>,
}

/// Service-owned branch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceBranchSessionRequest {
    /// Immutable parent.
    pub parent_session_id: agentmod_primitives::SessionId,
    /// Inclusive fork point.
    pub at: agentmod_primitives::Sequence,
    /// Optional child style replacement.
    pub style: Option<String>,
}

/// Service-owned branch response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceBranchSessionResponse {
    /// Fresh child.
    pub session_id: agentmod_primitives::SessionId,
    /// Immutable parent.
    pub parent_session_id: agentmod_primitives::SessionId,
    /// Parent fork point.
    pub fork_sequence: agentmod_primitives::Sequence,
    /// Child journal head.
    pub child_head_sequence: agentmod_primitives::Sequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceSchedule {
    schedule_id: String,
    session_id: agentmod_primitives::SessionId,
    idempotency_id: String,
    style: String,
    workspace: String,
    permission_policy: String,
    provider: String,
    model: String,
    token_budget: u64,
    cost_budget_micros: u64,
    trigger: ServiceScheduleTrigger,
    payload: ServiceSchedulePayload,
    active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ServiceScheduleTrigger {
    AtMillis(i64),
    Interval {
        starts_at_ms: i64,
        every_ms: u64,
    },
    RuntimeEvent {
        event_type: String,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ServiceSchedulePayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
    GraphTrigger { run_id: String, node_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceScheduleStoreResult {
    schedule_id: String,
    replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceScheduledExecution {
    execution_id: String,
    scheduled_for_ms: i64,
    claimed_at_ms: i64,
    observation: Option<ServiceScheduleObservation>,
    schedule: ServiceSchedule,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Service-owned exact observation attached to a scheduler claim.
pub enum ServiceScheduleObservation {
    /// Canonical committed runtime event.
    RuntimeEvent {
        /// Exact committed event identity.
        event_id: String,
    },
    /// Bounded process-output observation.
    ProcessOutput {
        /// Exact output observation identity.
        output_id: String,
    },
}

fn from_wire_schedule(value: RuntimeScheduleSpec) -> ServiceSchedule {
    ServiceSchedule {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            RuntimeScheduleTrigger::AtMillis(value) => ServiceScheduleTrigger::AtMillis(value),
            RuntimeScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ServiceScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            RuntimeScheduleTrigger::RuntimeEvent { event_type } => {
                ServiceScheduleTrigger::RuntimeEvent { event_type }
            }
            RuntimeScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ServiceScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            RuntimeSchedulePayload::Prompt { prompt } => ServiceSchedulePayload::Prompt { prompt },
            RuntimeSchedulePayload::Continuation { continuation_id } => {
                ServiceSchedulePayload::Continuation { continuation_id }
            }
            RuntimeSchedulePayload::GraphTrigger { run_id, node_id } => {
                ServiceSchedulePayload::GraphTrigger { run_id, node_id }
            }
        },
        active: value.active,
    }
}

fn to_logic_trigger(value: ServiceScheduleTrigger) -> ScheduleTrigger {
    match value {
        ServiceScheduleTrigger::AtMillis(value) => ScheduleTrigger::AtMillis(value),
        ServiceScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        } => ScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        },
        ServiceScheduleTrigger::RuntimeEvent { event_type } => {
            ScheduleTrigger::RuntimeEvent { event_type }
        }
        ServiceScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => ScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        },
    }
}

fn to_logic_payload(value: ServiceSchedulePayload) -> SchedulePayload {
    match value {
        ServiceSchedulePayload::Prompt { prompt } => SchedulePayload::Prompt { prompt },
        ServiceSchedulePayload::Continuation { continuation_id } => {
            SchedulePayload::Continuation { continuation_id }
        }
        ServiceSchedulePayload::GraphTrigger { run_id, node_id } => {
            SchedulePayload::GraphTrigger { run_id, node_id }
        }
    }
}

fn from_logic_schedule(value: RuntimeSchedule) -> ServiceSchedule {
    ServiceSchedule {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            ScheduleTrigger::AtMillis(value) => ServiceScheduleTrigger::AtMillis(value),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ServiceScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleTrigger::RuntimeEvent { event_type } => {
                ServiceScheduleTrigger::RuntimeEvent { event_type }
            }
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ServiceScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            SchedulePayload::Prompt { prompt } => ServiceSchedulePayload::Prompt { prompt },
            SchedulePayload::Continuation { continuation_id } => {
                ServiceSchedulePayload::Continuation { continuation_id }
            }
            SchedulePayload::GraphTrigger { run_id, node_id } => {
                ServiceSchedulePayload::GraphTrigger { run_id, node_id }
            }
        },
        active: value.active,
    }
}

const fn to_logic_style_decision(value: ServiceStyleDecisionCapability) -> StyleDecisionCapability {
    match value {
        ServiceStyleDecisionCapability::Continue => StyleDecisionCapability::Continue,
        ServiceStyleDecisionCapability::Replace => StyleDecisionCapability::Replace,
        ServiceStyleDecisionCapability::Reject => StyleDecisionCapability::Reject,
        ServiceStyleDecisionCapability::RequireApproval => StyleDecisionCapability::RequireApproval,
        ServiceStyleDecisionCapability::Defer => StyleDecisionCapability::Defer,
        ServiceStyleDecisionCapability::Cancel => StyleDecisionCapability::Cancel,
        ServiceStyleDecisionCapability::Fork => StyleDecisionCapability::Fork,
    }
}

const fn from_wire_manifest_format(
    value: RuntimeStyleManifestFormat,
) -> ServiceStyleManifestFormat {
    match value {
        RuntimeStyleManifestFormat::Toml => ServiceStyleManifestFormat::Toml,
        RuntimeStyleManifestFormat::Json => ServiceStyleManifestFormat::Json,
    }
}

fn from_logic_style_summary(value: StyleSummary) -> ServiceStyleSummary {
    ServiceStyleSummary {
        id: value.id,
        version: value.version,
        source: match value.source {
            StyleSource::BuiltIn => ServiceStyleSource::BuiltIn,
            StyleSource::User => ServiceStyleSource::User,
            StyleSource::Project => ServiceStyleSource::Project,
            StyleSource::Plugin => ServiceStyleSource::Plugin,
            StyleSource::Inline => ServiceStyleSource::Inline,
        },
        availability: match value.availability {
            StyleAvailability::Available => ServiceStyleAvailability::Available,
            StyleAvailability::Disabled => ServiceStyleAvailability::Disabled,
            StyleAvailability::Invalid => ServiceStyleAvailability::Invalid,
            StyleAvailability::Incompatible => ServiceStyleAvailability::Incompatible,
            StyleAvailability::Conflict => ServiceStyleAvailability::Conflict,
        },
        style_content_hash: value.content_hash.unwrap_or_default(),
        compiled_cache_key: value.compiled_cache_key.unwrap_or_default(),
        required_capabilities: value.required_capabilities,
    }
}

fn from_logic_style_inspection(value: StyleInspection) -> ServiceStyleInspection {
    ServiceStyleInspection {
        summary: from_logic_style_summary(value.summary),
        source_locator: value.source_locator,
        manifest: value.manifest,
        compiled: value.compiled,
        diagnostics: value
            .diagnostics
            .into_iter()
            .map(|diagnostic| ServiceStyleDiagnostic {
                code: diagnostic.code,
                path: diagnostic.path,
                message: diagnostic.message,
                help: diagnostic.help,
            })
            .collect(),
    }
}

fn to_wire_style_summary(value: ServiceStyleSummary) -> RuntimeStyleSummary {
    RuntimeStyleSummary {
        id: value.id,
        version: value.version,
        source: match value.source {
            ServiceStyleSource::BuiltIn => RuntimeStyleSourceKind::BuiltIn,
            ServiceStyleSource::User => RuntimeStyleSourceKind::User,
            ServiceStyleSource::Project => RuntimeStyleSourceKind::Project,
            ServiceStyleSource::Plugin => RuntimeStyleSourceKind::Plugin,
            ServiceStyleSource::Inline => RuntimeStyleSourceKind::Inline,
        },
        availability: match value.availability {
            ServiceStyleAvailability::Available => RuntimeStyleAvailability::Available,
            ServiceStyleAvailability::Disabled => RuntimeStyleAvailability::Disabled,
            ServiceStyleAvailability::Invalid => RuntimeStyleAvailability::Invalid,
            ServiceStyleAvailability::Incompatible => RuntimeStyleAvailability::Incompatible,
            ServiceStyleAvailability::Conflict => RuntimeStyleAvailability::Conflict,
        },
        style_content_hash: value.style_content_hash,
        compiled_cache_key: value.compiled_cache_key,
        required_capabilities: value.required_capabilities,
    }
}

fn to_wire_style_diagnostic(value: ServiceStyleDiagnostic) -> RuntimeStyleDiagnostic {
    RuntimeStyleDiagnostic {
        code: value.code,
        path: value.path,
        message: value.message,
        help: value.help,
    }
}

fn to_wire_style_inspection(value: ServiceStyleInspection) -> RuntimeStyleInspection {
    RuntimeStyleInspection {
        summary: to_wire_style_summary(value.summary),
        source_locator: value.source_locator,
        manifest: value.manifest,
        compiled: value.compiled,
        diagnostics: value
            .diagnostics
            .into_iter()
            .map(to_wire_style_diagnostic)
            .collect(),
    }
}

fn to_wire_harness_descriptor(value: HarnessDescriptor) -> RuntimeHarnessDescriptor {
    RuntimeHarnessDescriptor {
        id: value.id,
        version: value.version,
        capabilities: value.capabilities.into_iter().collect(),
        capability_set_hash: value.capability_set_hash.to_string(),
        availability: match value.availability {
            HarnessAvailability::Available => String::from("available"),
            HarnessAvailability::Disabled => String::from("disabled"),
        },
    }
}

fn from_logic_execution(value: ScheduledExecution) -> ServiceScheduledExecution {
    ServiceScheduledExecution {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        claimed_at_ms: value.claimed_at_ms,
        observation: value.observation.map(|observation| match observation {
            ScheduleObservation::RuntimeEvent { event_id } => {
                ServiceScheduleObservation::RuntimeEvent { event_id }
            }
            ScheduleObservation::ProcessOutput { output_id } => {
                ServiceScheduleObservation::ProcessOutput { output_id }
            }
        }),
        schedule: from_logic_schedule(value.schedule),
    }
}

fn to_wire_observation(value: ScheduleObservation) -> RuntimeScheduleObservation {
    match value {
        ScheduleObservation::RuntimeEvent { event_id } => {
            RuntimeScheduleObservation::RuntimeEvent { event_id }
        }
        ScheduleObservation::ProcessOutput { output_id } => {
            RuntimeScheduleObservation::ProcessOutput { output_id }
        }
    }
}

fn from_wire_observation(value: RuntimeScheduleObservation) -> ServiceScheduleObservation {
    match value {
        RuntimeScheduleObservation::RuntimeEvent { event_id } => {
            ServiceScheduleObservation::RuntimeEvent { event_id }
        }
        RuntimeScheduleObservation::ProcessOutput { output_id } => {
            ServiceScheduleObservation::ProcessOutput { output_id }
        }
    }
}

fn to_wire_schedule(value: ServiceSchedule) -> RuntimeScheduleSpec {
    RuntimeScheduleSpec {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            ServiceScheduleTrigger::AtMillis(value) => RuntimeScheduleTrigger::AtMillis(value),
            ServiceScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => RuntimeScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ServiceScheduleTrigger::RuntimeEvent { event_type } => {
                RuntimeScheduleTrigger::RuntimeEvent { event_type }
            }
            ServiceScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => RuntimeScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            ServiceSchedulePayload::Prompt { prompt } => RuntimeSchedulePayload::Prompt { prompt },
            ServiceSchedulePayload::Continuation { continuation_id } => {
                RuntimeSchedulePayload::Continuation { continuation_id }
            }
            ServiceSchedulePayload::GraphTrigger { run_id, node_id } => {
                RuntimeSchedulePayload::GraphTrigger { run_id, node_id }
            }
        },
        active: value.active,
    }
}

fn from_wire_mcp_server(value: WireMcpServerDeclaration) -> ServiceMcpServerDeclaration {
    ServiceMcpServerDeclaration {
        name: value.name,
        transport: match value.transport {
            WireMcpTransportDeclaration::Stdio {
                program,
                arguments,
                environment,
            } => ServiceMcpTransportDeclaration::Stdio {
                program,
                arguments,
                environment: environment
                    .into_iter()
                    .map(|entry| ServiceMcpSensitiveEntry {
                        name: entry.name,
                        value: entry.value,
                    })
                    .collect(),
            },
            WireMcpTransportDeclaration::StreamableHttp {
                url,
                legacy_sse,
                headers,
            } => ServiceMcpTransportDeclaration::StreamableHttp {
                url,
                legacy_sse,
                headers: headers
                    .into_iter()
                    .map(|entry| ServiceMcpSensitiveEntry {
                        name: entry.name,
                        value: entry.value,
                    })
                    .collect(),
            },
        },
    }
}

fn to_logic_mcp_server(value: ServiceMcpServerDeclaration) -> SessionMcpServerDeclaration {
    SessionMcpServerDeclaration {
        name: value.name,
        transport: match value.transport {
            ServiceMcpTransportDeclaration::Stdio {
                program,
                arguments,
                environment,
            } => SessionMcpTransportDeclaration::Stdio {
                program,
                arguments,
                environment: environment
                    .into_iter()
                    .map(|entry| SessionMcpSensitiveEntry {
                        name: entry.name,
                        value: entry.value,
                    })
                    .collect(),
            },
            ServiceMcpTransportDeclaration::StreamableHttp {
                url,
                legacy_sse,
                headers,
            } => SessionMcpTransportDeclaration::StreamableHttp {
                url,
                legacy_sse,
                headers: headers
                    .into_iter()
                    .map(|entry| SessionMcpSensitiveEntry {
                        name: entry.name,
                        value: entry.value,
                    })
                    .collect(),
            },
        },
    }
}

/// Runtime endpoint error.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ServiceError {
    /// Endpoint is part of the wire contract but not implemented in this slice.
    #[error("runtime endpoint is not available")]
    UnsupportedEndpoint,
    /// Service bootstrap/configuration supplied an invalid path.
    #[error("configured session root is empty")]
    InvalidSessionRoot,
    /// Business use case failed.
    #[error("runtime operation failed: {0}")]
    Logic(LogicError),
    /// Create request failed endpoint validation.
    #[error("create-session request is invalid")]
    InvalidSessionRequest,
    /// Platform cannot represent the requested list bound.
    #[error("session list limit is invalid")]
    InvalidSessionListLimit,
    /// Session registry business use case failed.
    #[error("session registry operation failed: {0}")]
    SessionRegistry(SessionRegistryLogicError),
    /// Session style discovery, validation, or selection failed.
    #[error("session style operation failed: {0}")]
    SessionStyle(SessionStyleLogicError),
    /// Harness discovery or inspection failed.
    #[error("harness registry operation failed: {0}")]
    HarnessRegistry(HarnessRegistryLogicError),
    /// Style endpoint request failed validation.
    #[error("session style request is invalid")]
    InvalidStyleRequest,
    /// A legacy session must be explicitly migrated before style execution.
    #[error("session style migration is required before execution can resume")]
    StyleMigrationRequired,
    /// Point-in-time replay or branching failed.
    #[error("session history operation failed: {0}")]
    SessionHistory(String),
    /// Durable scheduler operation failed.
    #[error("scheduler operation failed: {0}")]
    Schedule(RuntimeScheduleLogicError),
    /// Plugin lifecycle management failed.
    #[error("plugin lifecycle operation failed: {0}")]
    PluginLifecycle(String),
    /// Replay state could not be rendered at the endpoint boundary.
    #[error("session state could not be serialized")]
    StateSerialization,
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        sync::{Arc, Mutex},
    };

    use agentmod_runtime_logic::{
        RuntimeHealthResult,
        plugin_lifecycle::{
            ChangePluginLifecycleResult, PluginLifecycleError, PluginLifecycleLogicPort,
        },
    };
    use async_trait::async_trait;

    use super::*;

    struct MockLogic {
        state: RuntimeHealthState,
        observed: RefCell<Vec<GetRuntimeHealthCommand>>,
    }

    impl RuntimeLogicPort for MockLogic {
        fn get_health(
            &self,
            command: GetRuntimeHealthCommand,
        ) -> Result<RuntimeHealthResult, LogicError> {
            self.observed.borrow_mut().push(command);
            Ok(RuntimeHealthResult {
                state: self.state,
                diagnostics: vec![],
            })
        }
    }

    impl SessionRegistryLogicPort for MockLogic {
        fn create_session(
            &self,
            _command: CreateSessionCommand,
        ) -> Result<agentmod_runtime_logic::registry::CreateSessionResult, SessionRegistryLogicError>
        {
            Err(SessionRegistryLogicError::InvalidWorkspace)
        }

        fn list_sessions(
            &self,
            _command: ListSessionsCommand,
        ) -> Result<
            Vec<agentmod_runtime_logic::registry::SessionSummaryResult>,
            SessionRegistryLogicError,
        > {
            Ok(vec![])
        }
    }

    impl SessionHistoryLogicPort for MockLogic {
        fn inspect_session(
            &self,
            _command: InspectSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::InspectSessionResult,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }

        fn subscribe_session(
            &self,
            _command: agentmod_runtime_logic::history::SubscribeSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::SessionEventPage,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }

        fn branch_session(
            &self,
            _command: BranchSessionCommand,
        ) -> Result<
            agentmod_runtime_logic::history::BranchSessionResult,
            agentmod_runtime_logic::history::SessionHistoryLogicError,
        > {
            Err(agentmod_runtime_logic::history::SessionHistoryLogicError::InvalidSessionsRoot)
        }
    }

    impl SessionStyleLogicPort for MockLogic {
        fn list_styles(
            &self,
            _command: ListStylesCommand,
        ) -> Result<Vec<StyleSummary>, SessionStyleLogicError> {
            Ok(Vec::new())
        }

        fn inspect_style(
            &self,
            _command: InspectStyleCommand,
        ) -> Result<StyleInspection, SessionStyleLogicError> {
            Err(SessionStyleLogicError::InvalidSelector)
        }

        fn validate_style(
            &self,
            _command: ValidateStyleCommand,
        ) -> Result<StyleInspection, SessionStyleLogicError> {
            Err(SessionStyleLogicError::EmptyManifest)
        }

        fn resolve_style(
            &self,
            _command: InspectStyleCommand,
        ) -> Result<agentmod_runtime_logic::style::ResolvedStyle, SessionStyleLogicError> {
            Err(SessionStyleLogicError::InvalidSelector)
        }

        fn validate_style_binding(
            &self,
            _command: agentmod_runtime_logic::style::ValidateStyleBindingCommand,
        ) -> Result<(), SessionStyleLogicError> {
            Err(SessionStyleLogicError::InvalidSelector)
        }
    }

    impl HarnessRegistryLogicPort for MockLogic {
        fn list_harnesses(&self) -> Result<Vec<HarnessDescriptor>, HarnessRegistryLogicError> {
            Ok(Vec::new())
        }

        fn inspect_harness(
            &self,
            id: &str,
        ) -> Result<HarnessDescriptor, HarnessRegistryLogicError> {
            Err(HarnessRegistryLogicError::NotFound(id.to_owned()))
        }
    }

    fn service(state: RuntimeHealthState) -> RuntimeService<MockLogic> {
        RuntimeService::new(
            MockLogic {
                state,
                observed: RefCell::new(Vec::new()),
            },
            RuntimeServiceConfig {
                session_root: PathBuf::from("sessions"),
                version: "0.1.0-test".into(),
                styles: RuntimeStyleServiceConfig::native(std::path::Path::new("sessions")),
            },
        )
    }

    #[test]
    fn native_style_configuration_uses_the_logic_tool_catalog() {
        let config = RuntimeStyleServiceConfig::native(std::path::Path::new("sessions"));
        assert_eq!(config.tool_groups, canonical_tool_groups());
        assert_eq!(config.tool_groups.len(), 7);
        assert_eq!(
            config
                .tool_groups
                .values()
                .map(BTreeSet::len)
                .sum::<usize>(),
            57
        );
    }

    #[test]
    fn style_context_transform_catalog_maps_exactly_into_logic_environment() {
        let mut config = RuntimeStyleServiceConfig::native(std::path::Path::new("sessions"));
        config.context_transforms = vec![ServiceContextTransformDescriptor {
            plugin_id: String::from("fixture.context"),
            transform_id: String::from("fixture.redact"),
            version: String::from("1.2.3"),
            declaration_hash: agentmod_primitives::ContentHash::digest(b"declaration").to_hex(),
            lifecycle: String::from("before_model_request"),
        }];
        let environment = config.logic_environment(None);
        assert_eq!(environment.context_transforms.len(), 1);
        assert_eq!(
            environment.context_transforms[0],
            StyleContextTransformDescriptor {
                plugin_id: String::from("fixture.context"),
                transform_id: String::from("fixture.redact"),
                version: String::from("1.2.3"),
                declaration_hash: agentmod_primitives::ContentHash::digest(b"declaration").to_hex(),
                lifecycle: String::from("before_model_request"),
            }
        );

        config.plugin_memory_providers = vec![ServicePluginMemoryProviderDescriptor {
            plugin_id: String::from("fixture.memory"),
            plugin_version: String::from("3.0.0"),
            provider_id: String::from("fixture.semantic"),
            provider_version: String::from("1.4.0"),
            declaration_hash: agentmod_primitives::ContentHash::digest(b"memory").to_hex(),
            configuration_reference: agentmod_primitives::ContentHash::digest(
                b"memory-configuration",
            )
            .to_hex(),
            has_retrieve: true,
            has_write: true,
        }];
        config.plugin_compactors = vec![ServicePluginCompactorDescriptor {
            plugin_id: String::from("fixture.compaction"),
            plugin_version: String::from("4.0.0"),
            compactor_id: String::from("fixture.summary"),
            compactor_version: String::from("2.0.0"),
            declaration_hash: agentmod_primitives::ContentHash::digest(b"compactor").to_hex(),
            configuration_reference: agentmod_primitives::ContentHash::digest(
                b"compactor-configuration",
            )
            .to_hex(),
        }];
        let environment = config.logic_environment(None);
        assert_eq!(
            environment.plugin_memory_providers,
            vec![StylePluginMemoryProviderDescriptor {
                plugin_id: String::from("fixture.memory"),
                plugin_version: String::from("3.0.0"),
                provider_id: String::from("fixture.semantic"),
                provider_version: String::from("1.4.0"),
                declaration_hash: agentmod_primitives::ContentHash::digest(b"memory").to_hex(),
                configuration_reference: agentmod_primitives::ContentHash::digest(
                    b"memory-configuration",
                )
                .to_hex(),
                has_retrieve: true,
                has_write: true,
            }]
        );
        assert_eq!(
            environment.plugin_compactors,
            vec![StylePluginCompactorDescriptor {
                plugin_id: String::from("fixture.compaction"),
                plugin_version: String::from("4.0.0"),
                compactor_id: String::from("fixture.summary"),
                compactor_version: String::from("2.0.0"),
                declaration_hash: agentmod_primitives::ContentHash::digest(b"compactor").to_hex(),
                configuration_reference: agentmod_primitives::ContentHash::digest(
                    b"compactor-configuration",
                )
                .to_hex(),
            }]
        );
    }

    #[test]
    fn wire_health_is_mapped_through_service_and_logic_types() {
        let service = service(RuntimeHealthState::Ready);
        assert_eq!(
            service
                .handle_wire(&RuntimeRequest::Health)
                .expect("health"),
            RuntimeResponse::Health {
                status: "ok".into(),
                version: "0.1.0-test".into(),
            }
        );
        assert_eq!(
            service.logic.observed.into_inner(),
            vec![GetRuntimeHealthCommand {
                canonical_session_root: PathBuf::from("sessions")
            }]
        );
    }

    #[test]
    fn unsupported_wire_request_is_explicit() {
        assert_eq!(
            service(RuntimeHealthState::Ready).handle_wire(&RuntimeRequest::Cancel {
                cancellation_id: agentmod_primitives::CancellationId::from_uuid(
                    uuid::Uuid::from_u128(1),
                ),
                reason: String::from("fixture"),
            }),
            Err(ServiceError::UnsupportedEndpoint)
        );
    }

    #[derive(Clone, Default)]
    struct LifecycleMock {
        observed: Arc<Mutex<Vec<ChangePluginLifecycleCommand>>>,
    }

    #[async_trait]
    impl PluginLifecycleLogicPort for LifecycleMock {
        async fn change_plugin_lifecycle(
            &self,
            command: ChangePluginLifecycleCommand,
        ) -> Result<ChangePluginLifecycleResult, PluginLifecycleError> {
            self.observed
                .lock()
                .expect("lifecycle commands")
                .push(command.clone());
            Ok(ChangePluginLifecycleResult {
                plugin_id: command.plugin_id,
                plugin_version: String::from("1.2.3"),
                state: String::from("quarantined"),
                committed_sequence: agentmod_primitives::Sequence::new(4).expect("sequence"),
                replayed: false,
            })
        }
    }

    #[tokio::test]
    async fn plugin_lifecycle_service_maps_only_service_owned_types() {
        let logic = LifecycleMock::default();
        let service = RuntimeService::new(
            logic.clone(),
            RuntimeServiceConfig {
                session_root: PathBuf::from("sessions"),
                version: String::from("test"),
                styles: RuntimeStyleServiceConfig::native(std::path::Path::new("sessions")),
            },
        );
        let session_id = agentmod_primitives::SessionId::from_uuid(uuid::Uuid::from_u128(1));
        let cancellation_id =
            agentmod_primitives::CancellationId::from_uuid(uuid::Uuid::from_u128(2));
        let result = service
            .change_plugin_lifecycle(ServiceChangePluginLifecycleRequest {
                session_id,
                plugin_id: String::from("fixture.plugin"),
                action: ServicePluginLifecycleAction::Quarantine,
                reason_code: Some(String::from("integrity_failure")),
                cancellation_id,
            })
            .await
            .expect("quarantine");
        assert_eq!(result.state, "quarantined");
        assert_eq!(
            logic.observed.lock().expect("commands").as_slice(),
            &[ChangePluginLifecycleCommand {
                sessions_root: PathBuf::from("sessions"),
                session_id,
                plugin_id: String::from("fixture.plugin"),
                action: PluginLifecycleAction::Quarantine,
                reason_code: Some(String::from("integrity_failure")),
                cancellation_id: cancellation_id.to_string(),
            }]
        );
    }
}
