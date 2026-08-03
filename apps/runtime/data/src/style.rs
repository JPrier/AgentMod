//! Business-facing construction of the live session-style catalog.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use agentmod_primitives::ContentHash;
use agentmod_runtime_dependency::style::{
    DependencyStyleCacheLoadRequest, DependencyStyleCacheStoreRequest,
    DependencyStyleDiscoveryRequest, DependencyStyleManifestFormat, DependencyStyleSourceKind,
    SessionStyleDependencyError, SessionStyleDependencyPort,
};
use agentmod_session_style_sdk::{
    ApprovalDecision, AvailableContextTransform, AvailablePluginCompactor,
    AvailablePluginMemoryProvider, BuiltInStyle, CompileContext, ContextTransformLifecycle,
    DecisionCapability, ExecutionBudgetOverrides, ManifestFormat, SessionStyleManifest,
    StyleCompilerLimits, built_in_manifest_for_version, built_in_versions, compile_style,
    parse_json, parse_toml, select_child_session_restrictions, select_compaction_strategy,
    select_execution_budgets, select_memory_provider, to_json,
};
use serde::Serialize;
use thiserror::Error;

const MAX_IN_MEMORY_COMPILED_STYLES: usize = 256;

/// Stable diagnostic code for two sources claiming one exact style identity.
pub const DUPLICATE_STYLE_IDENTITY_DIAGNOSTIC_CODE: &str = "duplicate_style_identity";

/// Runtime availability inputs used exclusively for session-style compilation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStyleEnvironment {
    /// Running runtime API semantic version.
    pub runtime_api_version: String,
    /// Validated activated-plugin set content hash, encoded as lowercase BLAKE3 hex.
    pub plugin_set_hash: String,
    /// Available runtime capabilities.
    pub capabilities: BTreeSet<String>,
    /// Available tool groups and exact tools.
    pub tool_groups: BTreeMap<String, BTreeSet<String>>,
    /// Available provider IDs.
    pub providers: BTreeSet<String>,
    /// Available plugin IDs.
    pub plugins: BTreeSet<String>,
    /// Exact context-transform declarations from the authoritative plugin catalog.
    pub context_transforms: Vec<SessionStyleContextTransformDescriptor>,
    /// Exact memory-provider declarations from the authoritative plugin catalog.
    pub plugin_memory_providers: Vec<SessionStylePluginMemoryProviderDescriptor>,
    /// Exact compactor declarations from the authoritative plugin catalog.
    pub plugin_compactors: Vec<SessionStylePluginCompactorDescriptor>,
    /// Available memory provider IDs.
    pub memory_providers: BTreeSet<String>,
    /// Available compaction strategy IDs.
    pub compaction_strategies: BTreeSet<String>,
    /// Action/runtime decision kinds supported by the runtime API.
    pub supported_decisions: BTreeSet<SessionStyleDecisionCapability>,
    /// Resolved content-addressed graph source text.
    pub graph_references: BTreeMap<String, String>,
}

/// Data-owned exact plugin context-transform availability descriptor.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionStyleContextTransformDescriptor {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact transform identity.
    pub transform_id: String,
    /// Exact semantic version.
    pub version: String,
    /// Exact declaration hash as lowercase BLAKE3 hex.
    pub declaration_hash: String,
    /// Exact serialized lifecycle.
    pub lifecycle: String,
}

/// Data-owned exact plugin memory-provider availability descriptor.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionStylePluginMemoryProviderDescriptor {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Stable provider identity.
    pub provider_id: String,
    /// Exact provider version.
    pub provider_version: String,
    /// Hash of the exact validated declaration.
    pub declaration_hash: String,
    /// Exact canonical plugin configuration hash.
    pub configuration_reference: String,
    /// Whether retrieval is declared.
    pub has_retrieve: bool,
    /// Whether consequential write is declared.
    pub has_write: bool,
}

/// Data-owned exact plugin compactor availability descriptor.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionStylePluginCompactorDescriptor {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Stable compactor identity.
    pub compactor_id: String,
    /// Exact compactor version.
    pub compactor_version: String,
    /// Hash of the exact validated declaration.
    pub declaration_hash: String,
    /// Exact canonical plugin configuration hash.
    pub configuration_reference: String,
}

/// Data-owned decision capability made available by the running runtime API.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SessionStyleDecisionCapability {
    /// Continue with the proposal.
    Continue,
    /// Replace the proposal.
    Replace,
    /// Reject the proposal.
    Reject,
    /// Require durable approval.
    RequireApproval,
    /// Defer using a continuation.
    Defer,
    /// Cancel the proposal.
    Cancel,
    /// Fork supported execution.
    Fork,
}

/// Explicit roots and environment for the live style catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStyleCatalogDataRequest {
    /// Authoritative runtime availability inputs.
    pub environment: SessionStyleEnvironment,
    /// Optional user style root.
    pub user_style_root: Option<PathBuf>,
    /// Optional project style root.
    pub project_style_root: Option<PathBuf>,
    /// Activated plugin style roots.
    pub plugin_style_roots: Vec<PathBuf>,
    /// Optional explicit persistent-cache root.
    pub cache_root: Option<PathBuf>,
}

/// Layer-owned inline manifest encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum SessionStyleManifestFormat {
    /// TOML input.
    Toml,
    /// JSON input.
    Json,
}

/// Request to validate and compile a transient inline manifest without I/O writes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStyleValidationDataRequest {
    /// Authoritative runtime availability inputs.
    pub environment: SessionStyleEnvironment,
    /// Complete manifest source.
    pub manifest: String,
    /// Input encoding.
    pub format: SessionStyleManifestFormat,
}

/// Request to apply SDK-owned per-session component transforms and compile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStyleComponentSelectionDataRequest {
    /// Authoritative runtime availability inputs.
    pub environment: SessionStyleEnvironment,
    /// Canonical base manifest JSON.
    pub manifest: String,
    /// Optional memory-provider selection.
    pub memory: Option<String>,
    /// Optional compaction-strategy selection.
    pub compaction: Option<String>,
}

/// Request to apply SDK-owned per-session budget transforms and compile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStyleBudgetSelectionDataRequest {
    /// Authoritative runtime availability inputs.
    pub environment: SessionStyleEnvironment,
    /// Canonical base manifest JSON.
    pub manifest: String,
    /// Optional maximum loop/research iterations.
    pub max_iterations: Option<u32>,
    /// Optional maximum graph transitions.
    pub max_steps: Option<u64>,
    /// Optional maximum provider tokens.
    pub max_tokens: Option<u64>,
    /// Optional maximum cost in configured currency micros.
    pub max_cost_micros: Option<u64>,
    /// Optional maximum wall-clock duration.
    pub max_duration_ms: Option<u64>,
}

/// Data-owned child memory restriction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionStyleChildMemoryAccess {
    /// Child receives no memory access.
    None,
    /// Child may retrieve but cannot write memory.
    ReadOnly,
    /// Child retains the compiled style's read/write memory access.
    ReadWrite,
}

/// Request to compile one exact parent-restricted child-session style.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStyleChildSelectionDataRequest {
    /// Authoritative runtime availability inputs.
    pub environment: SessionStyleEnvironment,
    /// Canonical base manifest JSON.
    pub manifest: String,
    /// Exact tool groups granted by the parent.
    pub tool_groups: BTreeSet<String>,
    /// Exact parent-selected memory access.
    pub memory_access: SessionStyleChildMemoryAccess,
    /// Exact inherited provider selected by the parent, when enabled.
    pub inherited_provider: Option<String>,
    /// Optional maximum loop/research iterations.
    pub max_iterations: Option<u32>,
    /// Optional maximum graph transitions.
    pub max_steps: Option<u64>,
    /// Optional maximum provider tokens.
    pub max_tokens: Option<u64>,
    /// Optional maximum cost in configured currency micros.
    pub max_cost_micros: Option<u64>,
    /// Optional maximum wall-clock duration.
    pub max_duration_ms: Option<u64>,
}

/// Source category exposed to runtime logic and endpoints.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStyleSourceKind {
    /// Runtime-shipped semantic descriptor.
    BuiltIn,
    /// Per-user file.
    User,
    /// Project-local file.
    Project,
    /// Activated plugin file.
    Plugin,
    /// Transient caller-provided manifest.
    Inline,
}

/// Layer-owned style source details.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStyleSource {
    /// Stable display locator; never resolved by runtime logic.
    pub locator: String,
    /// Provenance category.
    pub kind: SessionStyleSourceKind,
    /// Original format, where relevant.
    pub format: Option<SessionStyleManifestFormat>,
    /// Exact source byte count.
    pub bytes: u64,
}

/// Catalog eligibility status. This does not select a style.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStyleCatalogStatus {
    /// SDK validation and compilation succeeded.
    Available,
    /// A discovered marker disabled this exact style ID.
    Disabled,
    /// Source parsing or non-compatibility validation failed.
    Invalid,
    /// The source requires unavailable runtime API/capability inputs.
    Incompatible,
}

/// Structured, SDK-derived diagnostic safe to expose at the data boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStyleDiagnostic {
    /// Stable SDK or data-discovery code.
    pub code: String,
    /// Manifest path or source locator.
    pub path: String,
    /// Human-readable problem.
    pub message: String,
    /// Actionable remediation when known.
    pub help: String,
}

/// Selected execution configuration, normalized into data-owned primitives.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStyleExecutionSelections {
    /// Selected memory provider.
    pub memory_provider: String,
    /// Exact plugin memory implementation, when selected.
    pub plugin_memory: Option<SessionStylePluginMemorySelection>,
    /// Selected memory scopes.
    pub memory_scopes: Vec<String>,
    /// Selected retrieval lifecycle boundary.
    pub memory_retrieval_timing: String,
    /// Canonical query construction.
    pub memory_query_json: String,
    /// Maximum memory records injected.
    pub memory_max_items: u32,
    /// Maximum injected memory bytes.
    pub memory_max_injected_bytes: u64,
    /// Selected automatic write boundary.
    pub memory_write_policy: String,
    /// Selected projection injection location.
    pub memory_injection_location: String,
    /// Selected compaction strategy.
    pub compaction_strategy: String,
    /// Exact plugin compactor implementation, when selected.
    pub plugin_compactor: Option<SessionStylePluginCompactorSelection>,
    /// Optional compaction token trigger.
    pub compaction_trigger_tokens: Option<u64>,
    /// Reserved non-history context budget.
    pub compaction_reserved_context_tokens: u64,
    /// Maximum compacted provider projection.
    pub compaction_max_provider_projection_tokens: u64,
    /// Whether unresolved tasks are preserved during compaction.
    pub compaction_preserve_unresolved_tasks: bool,
    /// Whether active process state is preserved during compaction.
    pub compaction_preserve_active_processes: bool,
    /// Typed preservation requirements.
    pub compaction_preservation_requirements: Vec<String>,
    /// Selected tool groups.
    pub tool_groups: Vec<String>,
    /// Ordered exact plugin context-transform selections.
    pub context_transforms: Vec<SessionStyleContextTransformSelection>,
    /// Hard iteration cap.
    pub max_iterations: u32,
    /// Hard step cap.
    pub max_steps: u64,
    /// Hard token cap.
    pub max_tokens: u64,
    /// Hard cost cap in micros.
    pub max_cost_micros: u64,
    /// Hard duration cap in milliseconds.
    pub max_duration_ms: u64,
    /// Default approval decision.
    pub default_approval: String,
    /// Approval overrides by action/tool group.
    pub approval_groups: BTreeMap<String, String>,
}

/// Data-owned immutable plugin memory selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStylePluginMemorySelection {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Stable provider identity.
    pub provider_id: String,
    /// Exact provider version.
    pub provider_version: String,
    /// Hash of the exact validated declaration.
    pub declaration_hash: String,
    /// Hash of the immutable adapter configuration.
    pub configuration_reference: String,
}

/// Data-owned immutable plugin compactor selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStylePluginCompactorSelection {
    /// Authoritative plugin identity.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Stable compactor identity.
    pub compactor_id: String,
    /// Exact compactor version.
    pub compactor_version: String,
    /// Hash of the exact validated declaration.
    pub declaration_hash: String,
    /// Hash of the immutable adapter configuration.
    pub configuration_reference: String,
}

/// Data-owned immutable selected plugin context transform.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStyleContextTransformSelection {
    /// Exact plugin identity.
    pub plugin_id: String,
    /// Exact transform identity.
    pub transform_id: String,
    /// Exact selected version.
    pub version: String,
    /// Exact declaration hash.
    pub declaration_hash: String,
    /// Exact lifecycle.
    pub lifecycle: String,
    /// Exact immutable adapter-configuration reference.
    pub configuration_reference: String,
}

/// One built or discovered style record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStyleCatalogRecord {
    /// Stable style identifier when parsing reached identity.
    pub id: Option<String>,
    /// Semantic style version when parsing reached identity.
    pub version: Option<String>,
    /// SDK-derived catalog eligibility.
    pub status: SessionStyleCatalogStatus,
    /// Original source metadata.
    pub source: SessionStyleSource,
    /// SDK/data diagnostics, deterministically sorted.
    pub diagnostics: Vec<SessionStyleDiagnostic>,
    /// Canonical SDK manifest JSON when parsing succeeded.
    pub canonical_manifest_json: Option<String>,
    /// Compiled SDK inspection JSON when compilation succeeded.
    pub compiled_json: Option<String>,
    /// Canonical manifest BLAKE3 hash when parsing succeeded.
    pub manifest_hash: Option<String>,
    /// SDK cache key binding every compilation input when compilation succeeded.
    pub cache_key: Option<String>,
    /// Compiled inspection JSON BLAKE3 hash when compilation succeeded.
    pub compiled_hash: Option<String>,
    /// Selected execution configuration when compilation succeeded.
    pub selections: Option<SessionStyleExecutionSelections>,
    /// Runtime API used for this catalog pass.
    pub runtime_api: String,
    /// Whether the compiled JSON was obtained from injected in-memory or persistent cache.
    pub cache_hit: bool,
}

/// Catalog result plus non-fatal source discovery diagnostics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionStyleCatalogDataRecord {
    /// Runtime API used for all records.
    pub runtime_api_version: String,
    /// Deterministically ordered built-in and discovered records.
    pub records: Vec<SessionStyleCatalogRecord>,
    /// Entries rejected before SDK parsing.
    pub discovery_errors: Vec<SessionStyleDiagnostic>,
}

/// In-memory compiled JSON cache element shared by `RuntimeData` clones.
#[derive(Clone, Debug)]
pub(crate) struct CachedSessionStyle {
    pub(crate) compiled_json: String,
}

/// Narrow catalog boundary consumed by runtime logic or endpoint adapters.
pub trait SessionStyleDataPort {
    /// Discovers, validates, and compiles the live style catalog.
    ///
    /// # Errors
    ///
    /// Returns an error when the injected dependency or environment is invalid.
    fn session_style_catalog(
        &self,
        request: SessionStyleCatalogDataRequest,
    ) -> Result<SessionStyleCatalogDataRecord, SessionStyleDataError>;

    /// Validates and compiles one transient inline manifest without filesystem writes.
    ///
    /// # Errors
    ///
    /// Returns an error when the environment is invalid.
    fn validate_session_style(
        &self,
        request: SessionStyleValidationDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError>;

    /// Applies SDK-owned component transforms and recompiles the manifest.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid typed selection or environment.
    fn select_session_style_components(
        &self,
        request: SessionStyleComponentSelectionDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError>;

    /// Applies SDK-owned budget transforms and recompiles the manifest.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid manifest or environment.
    fn select_session_style_budgets(
        &self,
        request: SessionStyleBudgetSelectionDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError>;

    /// Applies parent-owned child restrictions and recompiles the manifest.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid manifest, restriction, or environment.
    fn select_session_child_style(
        &self,
        request: SessionStyleChildSelectionDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError>;
}

/// Session-style data construction failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SessionStyleDataError {
    /// Dependency discovery or cache I/O failed.
    #[error("session-style dependency failed: {0}")]
    Dependency(#[from] SessionStyleDependencyError),
    /// An environment hash was not a valid BLAKE3 content hash.
    #[error("session-style plugin set hash is invalid")]
    InvalidPluginSetHash,
    /// The shared in-memory cache was poisoned.
    #[error("session-style in-memory cache is unavailable")]
    InMemoryCacheUnavailable,
    /// A compiled selection could not be normalized.
    #[error("session-style compiled selection serialization failed")]
    InvalidCompiledRecord,
    /// A component identifier is outside the SDK's typed selection model.
    #[error("session-style component selection is invalid: {0}")]
    InvalidComponentSelection(String),
}

impl<D> SessionStyleDataPort for super::RuntimeData<D>
where
    D: SessionStyleDependencyPort,
{
    fn session_style_catalog(
        &self,
        request: SessionStyleCatalogDataRequest,
    ) -> Result<SessionStyleCatalogDataRecord, SessionStyleDataError> {
        let context = compile_context(&request.environment)?;
        let discovery =
            self.dependency
                .discover_session_styles(DependencyStyleDiscoveryRequest {
                    user_root: request.user_style_root,
                    project_root: request.project_style_root,
                    plugin_roots: request.plugin_style_roots,
                })?;
        let disabled = discovery
            .disabled_markers
            .into_iter()
            .map(|marker| marker.style_id)
            .collect::<BTreeSet<_>>();
        let mut records = built_in_records(
            self,
            &request.environment,
            &context,
            request.cache_root.as_deref(),
            &disabled,
        )?;
        for source in discovery.manifests {
            records.push(compile_source(
                self,
                &request.environment,
                &context,
                request.cache_root.as_deref(),
                SessionStyleSource {
                    locator: source.source_locator,
                    kind: source_kind(source.source_kind),
                    format: Some(source.format.into()),
                    bytes: source.bytes,
                },
                source.contents,
                source.format.into(),
                &disabled,
                true,
            )?);
        }
        records.sort_by(|left, right| {
            left.id
                .cmp(&right.id)
                .then_with(|| left.version.cmp(&right.version))
                .then_with(|| left.source.kind.cmp(&right.source.kind))
                .then_with(|| left.source.locator.cmp(&right.source.locator))
        });
        add_duplicate_conflicts(&mut records);
        Ok(SessionStyleCatalogDataRecord {
            runtime_api_version: request.environment.runtime_api_version,
            records,
            discovery_errors: discovery
                .errors
                .into_iter()
                .map(|error| SessionStyleDiagnostic {
                    code: error.code.into(),
                    path: error.source_locator,
                    message: error.message,
                    help: "fix the source entry or remove it from the configured style root".into(),
                })
                .collect(),
        })
    }

    fn validate_session_style(
        &self,
        request: SessionStyleValidationDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError> {
        let context = compile_context(&request.environment)?;
        let source = SessionStyleSource {
            locator: "inline".into(),
            kind: SessionStyleSourceKind::Inline,
            format: Some(request.format),
            bytes: request.manifest.len() as u64,
        };
        compile_source(
            self,
            &request.environment,
            &context,
            None,
            source,
            request.manifest,
            request.format,
            &BTreeSet::new(),
            false,
        )
    }

    fn select_session_style_components(
        &self,
        request: SessionStyleComponentSelectionDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError> {
        let mut manifest = parse_json(&request.manifest)
            .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?;
        if let Some(memory) = request.memory.as_deref() {
            select_memory_provider(&mut manifest, memory);
        }
        if let Some(compaction) = request.compaction.as_deref() {
            select_compaction_strategy(&mut manifest, compaction).map_err(|_| {
                SessionStyleDataError::InvalidComponentSelection(compaction.to_owned())
            })?;
        }
        self.validate_session_style(SessionStyleValidationDataRequest {
            environment: request.environment,
            manifest: to_json(&manifest)
                .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
            format: SessionStyleManifestFormat::Json,
        })
    }

    fn select_session_style_budgets(
        &self,
        request: SessionStyleBudgetSelectionDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError> {
        let mut manifest = parse_json(&request.manifest)
            .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?;
        select_execution_budgets(
            &mut manifest,
            ExecutionBudgetOverrides {
                max_iterations: request.max_iterations,
                max_steps: request.max_steps,
                max_tokens: request.max_tokens,
                max_cost_micros: request.max_cost_micros,
                max_duration_ms: request.max_duration_ms,
            },
        )
        .map_err(|error| SessionStyleDataError::InvalidComponentSelection(error.to_string()))?;
        self.validate_session_style(SessionStyleValidationDataRequest {
            environment: request.environment,
            manifest: to_json(&manifest)
                .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
            format: SessionStyleManifestFormat::Json,
        })
    }

    fn select_session_child_style(
        &self,
        request: SessionStyleChildSelectionDataRequest,
    ) -> Result<SessionStyleCatalogRecord, SessionStyleDataError> {
        let mut manifest = parse_json(&request.manifest)
            .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?;
        select_child_session_restrictions(
            &mut manifest,
            &request.tool_groups,
            sdk_child_memory_access(request.memory_access),
            ExecutionBudgetOverrides {
                max_iterations: request.max_iterations,
                max_steps: request.max_steps,
                max_tokens: request.max_tokens,
                max_cost_micros: request.max_cost_micros,
                max_duration_ms: request.max_duration_ms,
            },
            request.inherited_provider.as_deref(),
        )
        .map_err(|error| SessionStyleDataError::InvalidComponentSelection(error.to_string()))?;
        self.validate_session_style(SessionStyleValidationDataRequest {
            environment: request.environment,
            manifest: to_json(&manifest)
                .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
            format: SessionStyleManifestFormat::Json,
        })
    }
}

const fn sdk_child_memory_access(
    value: SessionStyleChildMemoryAccess,
) -> agentmod_session_style_sdk::ChildMemoryAccess {
    match value {
        SessionStyleChildMemoryAccess::None => agentmod_session_style_sdk::ChildMemoryAccess::None,
        SessionStyleChildMemoryAccess::ReadOnly => {
            agentmod_session_style_sdk::ChildMemoryAccess::ReadOnly
        }
        SessionStyleChildMemoryAccess::ReadWrite => {
            agentmod_session_style_sdk::ChildMemoryAccess::ReadWrite
        }
    }
}

impl From<DependencyStyleManifestFormat> for SessionStyleManifestFormat {
    fn from(value: DependencyStyleManifestFormat) -> Self {
        match value {
            DependencyStyleManifestFormat::Toml => Self::Toml,
            DependencyStyleManifestFormat::Json => Self::Json,
        }
    }
}

fn built_in_records<D: SessionStyleDependencyPort>(
    data: &super::RuntimeData<D>,
    environment: &SessionStyleEnvironment,
    context: &CompileContext,
    cache_root: Option<&std::path::Path>,
    disabled: &BTreeSet<String>,
) -> Result<Vec<SessionStyleCatalogRecord>, SessionStyleDataError> {
    let built_ins = [
        BuiltInStyle::PersistentChat,
        BuiltInStyle::EphemeralTurn,
        BuiltInStyle::ResearchLoop,
        BuiltInStyle::PlannerWorker,
        BuiltInStyle::DeclarativeGraph,
    ];
    built_ins
        .into_iter()
        .flat_map(|style| {
            built_in_versions(style)
                .iter()
                .map(move |version| (style, *version))
        })
        .map(|manifest| {
            let (style, version) = manifest;
            let manifest = built_in_manifest_for_version(style, version)
                .ok_or(SessionStyleDataError::InvalidCompiledRecord)?;
            let contents = to_json(&manifest).unwrap_or_default();
            compile_source(
                data,
                environment,
                context,
                cache_root,
                SessionStyleSource {
                    locator: format!("built-in:{}", manifest.identity.id),
                    kind: SessionStyleSourceKind::BuiltIn,
                    format: Some(SessionStyleManifestFormat::Json),
                    bytes: contents.len() as u64,
                },
                contents,
                SessionStyleManifestFormat::Json,
                disabled,
                true,
            )
        })
        .collect()
}

#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    reason = "the catalog compilation boundary keeps source metadata, environment, cache, and disable state explicit"
)]
fn compile_source<D: SessionStyleDependencyPort>(
    data: &super::RuntimeData<D>,
    environment: &SessionStyleEnvironment,
    context: &CompileContext,
    cache_root: Option<&std::path::Path>,
    source: SessionStyleSource,
    contents: String,
    format: SessionStyleManifestFormat,
    disabled: &BTreeSet<String>,
    use_cache: bool,
) -> Result<SessionStyleCatalogRecord, SessionStyleDataError> {
    let manifest = match parse_manifest(&contents, format) {
        Ok(manifest) => manifest,
        Err(diagnostic) => return Ok(unparsed_record(environment, source, diagnostic)),
    };
    let canonical_manifest_json = to_json(&manifest).ok();
    let manifest_hash = canonical_manifest_json
        .as_deref()
        .map(|json| ContentHash::digest(json.as_bytes()).to_hex());
    let id = manifest.identity.id.clone();
    let version = manifest.identity.version.clone();
    if disabled.contains(&id) {
        return Ok(SessionStyleCatalogRecord {
            id: Some(id),
            version: Some(version),
            status: SessionStyleCatalogStatus::Disabled,
            source,
            diagnostics: vec![SessionStyleDiagnostic {
                code: "style_disabled".into(),
                path: "identity.id".into(),
                message: "a discovered .disabled marker disables this style ID".into(),
                help: "remove the marker to make the style eligible".into(),
            }],
            canonical_manifest_json,
            compiled_json: None,
            manifest_hash,
            cache_key: None,
            compiled_hash: None,
            selections: None,
            runtime_api: environment.runtime_api_version.clone(),
            cache_hit: false,
        });
    }
    match compile_style(&manifest, context, StyleCompilerLimits::default()) {
        Ok(compiled) => {
            let cache_key = compiled.cache_key.combined_hash.to_hex();
            let fresh_json = compiled.inspect_json().unwrap_or_default();
            let (compiled_json, cache_hit) =
                cached_json(data, cache_root, &cache_key, fresh_json, use_cache)?;
            Ok(SessionStyleCatalogRecord {
                id: Some(id),
                version: Some(version),
                status: SessionStyleCatalogStatus::Available,
                source,
                diagnostics: Vec::new(),
                canonical_manifest_json,
                manifest_hash,
                compiled_hash: Some(ContentHash::digest(compiled_json.as_bytes()).to_hex()),
                cache_key: Some(cache_key),
                selections: Some(selections(&compiled)?),
                compiled_json: Some(compiled_json),
                runtime_api: environment.runtime_api_version.clone(),
                cache_hit,
            })
        }
        Err(error) => {
            let diagnostics = error
                .diagnostics()
                .iter()
                .map(|diagnostic| SessionStyleDiagnostic {
                    code: diagnostic.code.into(),
                    path: diagnostic.path.clone(),
                    message: diagnostic.message.clone(),
                    help: diagnostic.help.clone(),
                })
                .collect::<Vec<_>>();
            let status = if diagnostics.iter().any(|diagnostic| {
                diagnostic.code.contains("runtime_api") || diagnostic.code.contains("unavailable")
            }) {
                SessionStyleCatalogStatus::Incompatible
            } else {
                SessionStyleCatalogStatus::Invalid
            };
            Ok(SessionStyleCatalogRecord {
                id: Some(id),
                version: Some(version),
                status,
                source,
                diagnostics,
                canonical_manifest_json,
                compiled_json: None,
                manifest_hash,
                cache_key: None,
                compiled_hash: None,
                selections: None,
                runtime_api: environment.runtime_api_version.clone(),
                cache_hit: false,
            })
        }
    }
}

fn cached_json<D: SessionStyleDependencyPort>(
    data: &super::RuntimeData<D>,
    cache_root: Option<&std::path::Path>,
    cache_key: &str,
    fresh_json: String,
    use_cache: bool,
) -> Result<(String, bool), SessionStyleDataError> {
    if !use_cache {
        return Ok((fresh_json, false));
    }
    if let Some(cached) = data
        .style_cache
        .lock()
        .map_err(|_| SessionStyleDataError::InMemoryCacheUnavailable)?
        .get(cache_key)
        .cloned()
    {
        return Ok((cached.compiled_json, true));
    }
    if let Some(root) = cache_root {
        if let Some(entry) =
            data.dependency
                .load_session_style_cache(DependencyStyleCacheLoadRequest {
                    cache_root: root.to_owned(),
                    cache_key: cache_key.into(),
                })?
            && serde_json::from_str::<agentmod_session_style_sdk::CompiledSessionStyle>(
                &entry.contents,
            )
            .is_ok_and(|compiled| compiled.cache_key.combined_hash.to_hex() == cache_key)
        {
            insert_memory_cache(data, cache_key, entry.contents.clone())?;
            return Ok((entry.contents, true));
        }
        data.dependency
            .store_session_style_cache(DependencyStyleCacheStoreRequest {
                cache_root: root.to_owned(),
                cache_key: cache_key.into(),
                contents: fresh_json.clone(),
            })?;
    }
    insert_memory_cache(data, cache_key, fresh_json.clone())?;
    Ok((fresh_json, false))
}

fn insert_memory_cache<D>(
    data: &super::RuntimeData<D>,
    cache_key: &str,
    compiled_json: String,
) -> Result<(), SessionStyleDataError> {
    let mut cache = data
        .style_cache
        .lock()
        .map_err(|_| SessionStyleDataError::InMemoryCacheUnavailable)?;
    if !cache.contains_key(cache_key)
        && cache.len() >= MAX_IN_MEMORY_COMPILED_STYLES
        && let Some(evicted) = cache.keys().next().cloned()
    {
        cache.remove(&evicted);
    }
    cache.insert(cache_key.into(), CachedSessionStyle { compiled_json });
    Ok(())
}

fn parse_manifest(
    contents: &str,
    format: SessionStyleManifestFormat,
) -> Result<SessionStyleManifest, SessionStyleDiagnostic> {
    let result = match format {
        SessionStyleManifestFormat::Toml => parse_toml(contents),
        SessionStyleManifestFormat::Json => parse_json(contents),
    };
    result.map_err(|error| SessionStyleDiagnostic {
        code: "manifest_parse".into(),
        path: "manifest".into(),
        message: error.message,
        help: match error.format {
            ManifestFormat::Toml => "supply strict session-style TOML".into(),
            ManifestFormat::Json => "supply strict session-style JSON".into(),
        },
    })
}

fn unparsed_record(
    environment: &SessionStyleEnvironment,
    source: SessionStyleSource,
    diagnostic: SessionStyleDiagnostic,
) -> SessionStyleCatalogRecord {
    SessionStyleCatalogRecord {
        id: None,
        version: None,
        status: SessionStyleCatalogStatus::Invalid,
        source,
        diagnostics: vec![diagnostic],
        canonical_manifest_json: None,
        compiled_json: None,
        manifest_hash: None,
        cache_key: None,
        compiled_hash: None,
        selections: None,
        runtime_api: environment.runtime_api_version.clone(),
        cache_hit: false,
    }
}

fn compile_context(
    environment: &SessionStyleEnvironment,
) -> Result<CompileContext, SessionStyleDataError> {
    Ok(CompileContext {
        runtime_api_version: environment.runtime_api_version.clone(),
        plugin_set_hash: environment
            .plugin_set_hash
            .parse()
            .map_err(|_| SessionStyleDataError::InvalidPluginSetHash)?,
        capabilities: environment.capabilities.clone(),
        tool_groups: environment.tool_groups.clone(),
        providers: environment.providers.clone(),
        plugins: environment.plugins.clone(),
        context_transforms: environment
            .context_transforms
            .iter()
            .map(|transform| {
                Ok(AvailableContextTransform {
                    plugin_id: transform.plugin_id.clone(),
                    transform_id: transform.transform_id.clone(),
                    version: transform.version.clone(),
                    declaration_hash: transform
                        .declaration_hash
                        .parse()
                        .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
                    lifecycle: match transform.lifecycle.as_str() {
                        "before_model_request" => ContextTransformLifecycle::BeforeModelRequest,
                        _ => return Err(SessionStyleDataError::InvalidCompiledRecord),
                    },
                })
            })
            .collect::<Result<Vec<_>, SessionStyleDataError>>()?,
        plugin_memory_providers: environment
            .plugin_memory_providers
            .iter()
            .map(|provider| {
                Ok(AvailablePluginMemoryProvider {
                    plugin_id: provider.plugin_id.clone(),
                    plugin_version: provider.plugin_version.clone(),
                    provider_id: provider.provider_id.clone(),
                    provider_version: provider.provider_version.clone(),
                    declaration_hash: provider
                        .declaration_hash
                        .parse()
                        .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
                    configuration_reference: provider
                        .configuration_reference
                        .parse()
                        .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
                    has_retrieve: provider.has_retrieve,
                    has_write: provider.has_write,
                })
            })
            .collect::<Result<Vec<_>, SessionStyleDataError>>()?,
        plugin_compactors: environment
            .plugin_compactors
            .iter()
            .map(|compactor| {
                Ok(AvailablePluginCompactor {
                    plugin_id: compactor.plugin_id.clone(),
                    plugin_version: compactor.plugin_version.clone(),
                    compactor_id: compactor.compactor_id.clone(),
                    compactor_version: compactor.compactor_version.clone(),
                    declaration_hash: compactor
                        .declaration_hash
                        .parse()
                        .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
                    configuration_reference: compactor
                        .configuration_reference
                        .parse()
                        .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
                })
            })
            .collect::<Result<Vec<_>, SessionStyleDataError>>()?,
        memory_providers: environment.memory_providers.clone(),
        compaction_strategies: environment.compaction_strategies.clone(),
        supported_decisions: environment
            .supported_decisions
            .iter()
            .copied()
            .map(decision_capability)
            .collect(),
        graph_references: environment.graph_references.clone(),
    })
}

fn source_kind(kind: DependencyStyleSourceKind) -> SessionStyleSourceKind {
    match kind {
        DependencyStyleSourceKind::User => SessionStyleSourceKind::User,
        DependencyStyleSourceKind::Project => SessionStyleSourceKind::Project,
        DependencyStyleSourceKind::Plugin => SessionStyleSourceKind::Plugin,
    }
}

fn selections(
    compiled: &agentmod_session_style_sdk::CompiledSessionStyle,
) -> Result<SessionStyleExecutionSelections, SessionStyleDataError> {
    Ok(SessionStyleExecutionSelections {
        memory_provider: compiled.memory.provider.clone(),
        plugin_memory: compiled.memory.plugin.as_ref().map(|plugin| {
            SessionStylePluginMemorySelection {
                plugin_id: plugin.plugin_id.clone(),
                plugin_version: plugin.plugin_version.clone(),
                provider_id: plugin.provider_id.clone(),
                provider_version: plugin.provider_version.clone(),
                declaration_hash: plugin.declaration_hash.to_hex(),
                configuration_reference: plugin.configuration_reference.to_hex(),
            }
        }),
        memory_scopes: compiled
            .memory
            .scopes
            .iter()
            .map(serialized_enum_name)
            .collect::<Result<_, _>>()?,
        memory_retrieval_timing: serialized_enum_name(&compiled.memory.retrieval_timing)?,
        memory_query_json: serde_json::to_string(&compiled.memory.query)
            .map_err(|_| SessionStyleDataError::InvalidCompiledRecord)?,
        memory_max_items: compiled.memory.max_items,
        memory_max_injected_bytes: compiled.memory.max_injected_bytes,
        memory_write_policy: serialized_enum_name(&compiled.memory.write_policy)?,
        memory_injection_location: serialized_enum_name(&compiled.memory.injection_location)?,
        compaction_strategy: serialized_enum_name(&compiled.compaction.strategy)?,
        plugin_compactor: compiled.compaction.plugin.as_ref().map(|plugin| {
            SessionStylePluginCompactorSelection {
                plugin_id: plugin.plugin_id.clone(),
                plugin_version: plugin.plugin_version.clone(),
                compactor_id: plugin.compactor_id.clone(),
                compactor_version: plugin.compactor_version.clone(),
                declaration_hash: plugin.declaration_hash.to_hex(),
                configuration_reference: plugin.configuration_reference.to_hex(),
            }
        }),
        compaction_trigger_tokens: compiled.compaction.trigger_tokens,
        compaction_reserved_context_tokens: compiled.compaction.reserved_context_tokens,
        compaction_max_provider_projection_tokens: compiled
            .compaction
            .max_provider_projection_tokens,
        compaction_preserve_unresolved_tasks: compiled.compaction.preserve_unresolved_tasks,
        compaction_preserve_active_processes: compiled.compaction.preserve_active_processes,
        compaction_preservation_requirements: compiled
            .compaction
            .preservation_requirements
            .iter()
            .map(serialized_enum_name)
            .collect::<Result<_, _>>()?,
        tool_groups: compiled.allowed_tool_groups.clone(),
        context_transforms: compiled
            .context_transforms
            .iter()
            .map(|transform| {
                Ok(SessionStyleContextTransformSelection {
                    plugin_id: transform.plugin_id.clone(),
                    transform_id: transform.transform_id.clone(),
                    version: transform.version.clone(),
                    declaration_hash: transform.declaration_hash.to_hex(),
                    lifecycle: serialized_enum_name(&transform.lifecycle)?,
                    configuration_reference: transform.configuration_reference.to_hex(),
                })
            })
            .collect::<Result<Vec<_>, SessionStyleDataError>>()?,
        max_iterations: compiled.budgets.max_iterations,
        max_steps: compiled.budgets.max_steps,
        max_tokens: compiled.budgets.max_tokens,
        max_cost_micros: compiled.budgets.max_cost_micros,
        max_duration_ms: compiled.budgets.max_duration_ms,
        default_approval: approval(compiled.approvals.default).into(),
        approval_groups: compiled
            .approvals
            .groups
            .iter()
            .map(|(key, value)| (key.clone(), approval(*value).into()))
            .collect(),
    })
}

fn serialized_enum_name<T: Serialize>(value: &T) -> Result<String, SessionStyleDataError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or(SessionStyleDataError::InvalidCompiledRecord)
}

const fn decision_capability(value: SessionStyleDecisionCapability) -> DecisionCapability {
    match value {
        SessionStyleDecisionCapability::Continue => DecisionCapability::Continue,
        SessionStyleDecisionCapability::Replace => DecisionCapability::Replace,
        SessionStyleDecisionCapability::Reject => DecisionCapability::Reject,
        SessionStyleDecisionCapability::RequireApproval => DecisionCapability::RequireApproval,
        SessionStyleDecisionCapability::Defer => DecisionCapability::Defer,
        SessionStyleDecisionCapability::Cancel => DecisionCapability::Cancel,
        SessionStyleDecisionCapability::Fork => DecisionCapability::Fork,
    }
}

const fn approval(value: ApprovalDecision) -> &'static str {
    match value {
        ApprovalDecision::Allow => "allow",
        ApprovalDecision::Ask => "ask",
        ApprovalDecision::Deny => "deny",
    }
}

fn add_duplicate_conflicts(records: &mut [SessionStyleCatalogRecord]) {
    let mut counts = BTreeMap::<(String, String), usize>::new();
    for record in records.iter() {
        if let (Some(id), Some(version)) = (&record.id, &record.version) {
            *counts.entry((id.clone(), version.clone())).or_default() += 1;
        }
    }
    for record in records {
        if let (Some(id), Some(version)) = (&record.id, &record.version)
            && counts
                .get(&(id.clone(), version.clone()))
                .copied()
                .unwrap_or_default()
                > 1
        {
            if record.status != SessionStyleCatalogStatus::Disabled {
                record.status = SessionStyleCatalogStatus::Invalid;
            }
            record.diagnostics.push(SessionStyleDiagnostic {
                code: DUPLICATE_STYLE_IDENTITY_DIAGNOSTIC_CODE.into(),
                path: "identity".into(),
                message: format!("multiple sources define style `{id}` version `{version}`"),
                help: "retain exactly one source for this style ID and version".into(),
            });
            record.diagnostics.sort_by(|left, right| {
                left.code
                    .cmp(&right.code)
                    .then_with(|| left.path.cmp(&right.path))
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap};

    use super::*;
    use agentmod_runtime_dependency::style::{
        DependencyStyleCacheLoadRequest, DependencyStyleCacheRecord,
        DependencyStyleCacheStoreRequest, DependencyStyleDisabledMarker, DependencyStyleDiscovery,
        DependencyStyleManifestRecord,
    };

    #[derive(Default)]
    struct MockDependency {
        discovery: DependencyStyleDiscovery,
        cache: RefCell<BTreeMap<String, String>>,
    }

    impl SessionStyleDependencyPort for MockDependency {
        fn discover_session_styles(
            &self,
            _request: DependencyStyleDiscoveryRequest,
        ) -> Result<DependencyStyleDiscovery, SessionStyleDependencyError> {
            Ok(self.discovery.clone())
        }

        fn load_session_style_cache(
            &self,
            request: DependencyStyleCacheLoadRequest,
        ) -> Result<Option<DependencyStyleCacheRecord>, SessionStyleDependencyError> {
            Ok(self.cache.borrow().get(&request.cache_key).map(|contents| {
                DependencyStyleCacheRecord {
                    cache_key: request.cache_key,
                    bytes: contents.len() as u64,
                    contents: contents.clone(),
                }
            }))
        }

        fn store_session_style_cache(
            &self,
            request: DependencyStyleCacheStoreRequest,
        ) -> Result<(), SessionStyleDependencyError> {
            self.cache
                .borrow_mut()
                .insert(request.cache_key, request.contents);
            Ok(())
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the test environment spells out the complete service-provided native tool catalog"
    )]
    fn environment() -> SessionStyleEnvironment {
        SessionStyleEnvironment {
            runtime_api_version: "1.0.0".into(),
            plugin_set_hash: ContentHash::digest(b"plugins").to_hex(),
            capabilities: [
                "agents",
                "approval",
                "artifacts",
                "context",
                "events",
                "model",
                "tools",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
            tool_groups: BTreeMap::from([
                (
                    "browser".into(),
                    [
                        "browser.start",
                        "browser.navigate",
                        "browser.inspect",
                        "browser.screenshot",
                        "browser.click",
                        "browser.type",
                        "browser.submit",
                        "browser.download",
                        "browser.close",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                ),
                (
                    "filesystem".into(),
                    [
                        "filesystem.read",
                        "filesystem.list",
                        "filesystem.glob",
                        "filesystem.grep",
                        "filesystem.write",
                        "filesystem.edit",
                        "filesystem.apply_patch",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                ),
                (
                    "git".into(),
                    [
                        "git.discover",
                        "git.status",
                        "git.diff",
                        "git.changed_files",
                        "git.branch",
                        "git.dirty",
                        "git.worktree_create",
                        "git.worktree_cleanup",
                        "git.checkpoint_create",
                        "git.checkpoint_restore",
                        "git.export_patch",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                ),
                (
                    "lsp".into(),
                    [
                        "lsp.project_root",
                        "lsp.diagnostics",
                        "lsp.document_symbols",
                        "lsp.workspace_symbols",
                        "lsp.definition",
                        "lsp.references",
                        "lsp.hover",
                        "lsp.signature_help",
                        "lsp.rename",
                        "lsp.formatting",
                        "lsp.code_actions",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                ),
                (
                    "mcp".into(),
                    ["mcp.server.list", "mcp.capabilities", "mcp.invoke"]
                        .into_iter()
                        .map(String::from)
                        .collect(),
                ),
                (
                    "process".into(),
                    [
                        "process.run",
                        "process.start",
                        "process.run_pty",
                        "process.start_pty",
                        "process.read",
                        "process.input",
                        "process.resize",
                        "process.wait",
                        "process.interrupt",
                        "process.kill",
                        "process.detach",
                        "process.reattach",
                        "process.list",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                ),
                (
                    "web".into(),
                    ["http.request", "web.fetch", "web.search"]
                        .into_iter()
                        .map(String::from)
                        .collect(),
                ),
            ]),
            providers: ["deterministic-mock".into(), "mock".into()]
                .into_iter()
                .collect(),
            plugins: ["runtime.security".into()].into_iter().collect(),
            context_transforms: Vec::new(),
            plugin_memory_providers: Vec::new(),
            plugin_compactors: Vec::new(),
            memory_providers: ["file".into(), "none".into()].into_iter().collect(),
            compaction_strategies: ["summary".into(), "artifact_handoff".into(), "none".into()]
                .into_iter()
                .collect(),
            supported_decisions: [
                SessionStyleDecisionCapability::Continue,
                SessionStyleDecisionCapability::Replace,
                SessionStyleDecisionCapability::Reject,
                SessionStyleDecisionCapability::RequireApproval,
                SessionStyleDecisionCapability::Cancel,
            ]
            .into_iter()
            .collect(),
            graph_references: BTreeMap::new(),
        }
    }

    fn catalog_request() -> SessionStyleCatalogDataRequest {
        SessionStyleCatalogDataRequest {
            environment: environment(),
            user_style_root: None,
            project_style_root: None,
            plugin_style_roots: Vec::new(),
            cache_root: None,
        }
    }

    #[test]
    fn all_built_ins_are_available_and_cache_hits_after_first_read() {
        let data = super::super::RuntimeData::new(MockDependency::default());
        let first = data
            .session_style_catalog(catalog_request())
            .expect("catalog");
        assert_eq!(first.records.len(), 13);
        assert!(
            first
                .records
                .iter()
                .all(|record| record.status == SessionStyleCatalogStatus::Available)
        );
        let persistent = first
            .records
            .iter()
            .find(|record| record.id.as_deref() == Some("persistent-chat"))
            .and_then(|record| record.selections.as_ref())
            .expect("persistent selections");
        assert_eq!(persistent.memory_retrieval_timing, "turn_start");
        assert_eq!(persistent.memory_write_policy, "turn_completion");
        assert_eq!(persistent.memory_injection_location, "before_current_input");
        assert_eq!(persistent.compaction_strategy, "summary");
        assert!(
            persistent
                .compaction_preservation_requirements
                .contains(&String::from("memory_provenance"))
        );
        let second = data
            .session_style_catalog(catalog_request())
            .expect("cached catalog");
        assert!(second.records.iter().all(|record| record.cache_hit));
    }

    #[test]
    fn catalog_exposes_current_and_historical_built_ins_exactly() {
        let data = super::super::RuntimeData::new(MockDependency::default());
        let catalog = data
            .session_style_catalog(catalog_request())
            .expect("catalog");

        for (style, id, versions) in [
            (
                BuiltInStyle::PersistentChat,
                "persistent-chat",
                &["1.1.0", "1.2.0"][..],
            ),
            (
                BuiltInStyle::EphemeralTurn,
                "ephemeral-turn",
                &["1.1.0", "1.2.0"][..],
            ),
            (
                BuiltInStyle::ResearchLoop,
                "research-loop",
                &["1.1.0", "1.2.0", "1.3.0"][..],
            ),
            (
                BuiltInStyle::DeclarativeGraph,
                "declarative-graph",
                &["1.1.0", "1.2.0"][..],
            ),
            (
                BuiltInStyle::PlannerWorker,
                "planner-worker",
                &["1.1.0", "1.2.0", "1.3.0", "1.4.0"][..],
            ),
        ] {
            let exposed = catalog
                .records
                .iter()
                .filter(|record| record.id.as_deref() == Some(id))
                .map(|record| record.version.as_deref().expect("version"))
                .collect::<BTreeSet<_>>();
            assert_eq!(exposed, versions.iter().copied().collect());

            for &version in versions {
                let record = catalog
                    .records
                    .iter()
                    .find(|record| {
                        record.id.as_deref() == Some(id)
                            && record.version.as_deref() == Some(version)
                    })
                    .expect("exact built-in version");
                assert_eq!(record.status, SessionStyleCatalogStatus::Available);
                assert_eq!(record.source.kind, SessionStyleSourceKind::BuiltIn);
                let expected =
                    built_in_manifest_for_version(style, version).expect("shipped version");
                assert_eq!(
                    record.canonical_manifest_json.as_deref(),
                    to_json(&expected).ok().as_deref()
                );
            }
        }
    }

    #[test]
    fn catalog_does_not_synthesize_unknown_built_in_versions() {
        let data = super::super::RuntimeData::new(MockDependency::default());
        let catalog = data
            .session_style_catalog(catalog_request())
            .expect("catalog");

        for (id, version) in [
            ("ephemeral-turn", "1.0.0"),
            ("ephemeral-turn", "1.3.0"),
            ("persistent-chat", "1.3.0"),
            ("declarative-graph", "1.0.0"),
            ("declarative-graph", "1.3.0"),
            ("research-loop", "1.0.0"),
            ("research-loop", "1.4.0"),
            ("planner-worker", "1.0.0"),
            ("planner-worker", "1.5.0"),
        ] {
            assert!(!catalog.records.iter().any(|record| {
                record.id.as_deref() == Some(id) && record.version.as_deref() == Some(version)
            }));
        }
    }

    #[test]
    fn component_selection_is_transformed_and_compiled_inside_data() {
        let data = super::super::RuntimeData::new(MockDependency::default());
        let manifest = agentmod_session_style_sdk::to_json(
            &agentmod_session_style_sdk::built_in_manifest(BuiltInStyle::EphemeralTurn),
        )
        .expect("manifest");
        let record = data
            .select_session_style_components(SessionStyleComponentSelectionDataRequest {
                environment: environment(),
                manifest,
                memory: Some(String::from("file")),
                compaction: Some(String::from("artifact_handoff")),
            })
            .expect("selection");

        assert_eq!(record.status, SessionStyleCatalogStatus::Available);
        let selections = record.selections.expect("compiled selections");
        assert_eq!(selections.memory_provider, "file");
        assert_eq!(selections.compaction_strategy, "artifact_handoff");
        assert!(record.compiled_hash.is_some());
        assert!(record.cache_key.is_some());
    }

    #[test]
    fn ordered_context_transform_selection_survives_fresh_runtime_compilation() {
        let declaration_hash = ContentHash::digest(b"runtime transform declaration");
        let configuration_reference = ContentHash::digest(b"runtime transform configuration");
        let mut manifest =
            agentmod_session_style_sdk::built_in_manifest(BuiltInStyle::PersistentChat);
        manifest
            .allowed_plugins
            .push(String::from("fixture.context"));
        manifest.context_transforms = vec![agentmod_session_style_sdk::ContextTransformSelection {
            plugin_id: String::from("fixture.context"),
            transform_id: String::from("fixture.redact"),
            version: String::from("1.0.0"),
            declaration_hash,
            lifecycle: agentmod_session_style_sdk::ContextTransformLifecycle::BeforeModelRequest,
            configuration_reference,
        }];
        let manifest =
            agentmod_session_style_sdk::to_json(&manifest).expect("canonical manifest JSON");
        let mut runtime = environment();
        runtime.plugins.insert(String::from("fixture.context"));
        runtime.context_transforms = vec![SessionStyleContextTransformDescriptor {
            plugin_id: String::from("fixture.context"),
            transform_id: String::from("fixture.redact"),
            version: String::from("1.0.0"),
            declaration_hash: declaration_hash.to_hex(),
            lifecycle: String::from("before_model_request"),
        }];

        let compile = |data: &super::super::RuntimeData<MockDependency>| {
            data.validate_session_style(SessionStyleValidationDataRequest {
                environment: runtime.clone(),
                manifest: manifest.clone(),
                format: SessionStyleManifestFormat::Json,
            })
            .expect("runtime style compilation")
        };
        let first_data = super::super::RuntimeData::new(MockDependency::default());
        let first = compile(&first_data);
        assert_eq!(first.status, SessionStyleCatalogStatus::Available);
        let selected = &first
            .selections
            .as_ref()
            .expect("compiled selections")
            .context_transforms;
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].plugin_id, "fixture.context");
        assert_eq!(selected[0].transform_id, "fixture.redact");
        assert_eq!(selected[0].version, "1.0.0");
        assert_eq!(selected[0].declaration_hash, declaration_hash.to_hex());
        assert_eq!(
            selected[0].configuration_reference,
            configuration_reference.to_hex()
        );

        let restarted_data = super::super::RuntimeData::new(MockDependency::default());
        let restarted = compile(&restarted_data);
        assert_eq!(restarted.cache_key, first.cache_key);
        assert_eq!(restarted.compiled_hash, first.compiled_hash);
        assert_eq!(
            restarted
                .selections
                .expect("restart selections")
                .context_transforms,
            *selected
        );
        let compiled: agentmod_session_style_sdk::CompiledSessionStyle = serde_json::from_str(
            restarted
                .compiled_json
                .as_deref()
                .expect("compiled binding payload"),
        )
        .expect("compiled binding reload");
        assert_eq!(compiled.context_transforms.len(), 1);
        assert_eq!(
            compiled.context_transforms[0].configuration_reference,
            configuration_reference
        );
    }

    #[test]
    fn exact_plugin_memory_and_compactor_selections_survive_fresh_runtime_compilation() {
        let memory_hash = ContentHash::digest(b"exact memory declaration");
        let memory_configuration = ContentHash::digest(b"exact memory configuration");
        let compactor_hash = ContentHash::digest(b"exact compactor declaration");
        let compactor_configuration = ContentHash::digest(b"exact compactor configuration");
        let mut manifest =
            agentmod_session_style_sdk::built_in_manifest(BuiltInStyle::PersistentChat);
        manifest
            .allowed_plugins
            .push(String::from("fixture.context"));
        manifest.memory.provider = String::from("fixture.memory");
        manifest.memory.plugin = Some(agentmod_session_style_sdk::PluginMemorySelection {
            plugin_id: String::from("fixture.context"),
            plugin_version: String::from("2.3.4"),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.4.0"),
            declaration_hash: memory_hash,
            configuration_reference: memory_configuration,
        });
        manifest.compaction.strategy = agentmod_session_style_sdk::CompactionStrategy::Plugin;
        manifest.compaction.plugin = Some(agentmod_session_style_sdk::PluginCompactorSelection {
            plugin_id: String::from("fixture.context"),
            plugin_version: String::from("2.3.4"),
            compactor_id: String::from("fixture.compactor"),
            compactor_version: String::from("3.1.0"),
            declaration_hash: compactor_hash,
            configuration_reference: compactor_configuration,
        });
        let manifest =
            agentmod_session_style_sdk::to_json(&manifest).expect("canonical manifest JSON");
        let mut runtime = environment();
        runtime.plugins.insert(String::from("fixture.context"));
        runtime.plugin_memory_providers = vec![SessionStylePluginMemoryProviderDescriptor {
            plugin_id: String::from("fixture.context"),
            plugin_version: String::from("2.3.4"),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.4.0"),
            declaration_hash: memory_hash.to_hex(),
            configuration_reference: memory_configuration.to_hex(),
            has_retrieve: true,
            has_write: true,
        }];
        runtime.plugin_compactors = vec![SessionStylePluginCompactorDescriptor {
            plugin_id: String::from("fixture.context"),
            plugin_version: String::from("2.3.4"),
            compactor_id: String::from("fixture.compactor"),
            compactor_version: String::from("3.1.0"),
            declaration_hash: compactor_hash.to_hex(),
            configuration_reference: compactor_configuration.to_hex(),
        }];

        let compile = |data: &super::super::RuntimeData<MockDependency>| {
            data.validate_session_style(SessionStyleValidationDataRequest {
                environment: runtime.clone(),
                manifest: manifest.clone(),
                format: SessionStyleManifestFormat::Json,
            })
            .expect("runtime style compilation")
        };
        let first = compile(&super::super::RuntimeData::new(MockDependency::default()));
        assert_eq!(first.status, SessionStyleCatalogStatus::Available);
        let selections = first.selections.as_ref().expect("compiled selections");
        assert_eq!(
            selections
                .plugin_memory
                .as_ref()
                .expect("plugin memory")
                .declaration_hash,
            memory_hash.to_hex()
        );
        assert_eq!(
            selections
                .plugin_compactor
                .as_ref()
                .expect("plugin compactor")
                .configuration_reference,
            compactor_configuration.to_hex()
        );

        let restarted = compile(&super::super::RuntimeData::new(MockDependency::default()));
        assert_eq!(restarted.cache_key, first.cache_key);
        assert_eq!(restarted.compiled_hash, first.compiled_hash);
        assert_eq!(restarted.selections, first.selections);
    }

    #[test]
    fn budget_selection_is_transformed_and_compiled_inside_data() {
        let data = super::super::RuntimeData::new(MockDependency::default());
        let manifest = agentmod_session_style_sdk::to_json(
            &agentmod_session_style_sdk::built_in_manifest(BuiltInStyle::EphemeralTurn),
        )
        .expect("manifest");
        let record = data
            .select_session_style_budgets(SessionStyleBudgetSelectionDataRequest {
                environment: environment(),
                manifest,
                max_iterations: Some(3),
                max_steps: Some(40),
                max_tokens: Some(100_000),
                max_cost_micros: Some(1_000_000),
                max_duration_ms: Some(60_000),
            })
            .expect("selection");

        assert_eq!(record.status, SessionStyleCatalogStatus::Available);
        let budgets = record.selections.expect("compiled selections");
        assert_eq!(budgets.max_iterations, 3);
        assert_eq!(budgets.max_steps, 40);
        assert_eq!(budgets.max_tokens, 100_000);
        assert_eq!(budgets.max_cost_micros, 1_000_000);
        assert_eq!(budgets.max_duration_ms, 60_000);
        assert!(record.compiled_hash.is_some());
        assert!(record.cache_key.is_some());
    }

    #[test]
    fn child_selection_compiles_parent_limits_and_inherited_provider() {
        let data = super::super::RuntimeData::new(MockDependency::default());
        let manifest = agentmod_session_style_sdk::to_json(
            &agentmod_session_style_sdk::built_in_manifest(BuiltInStyle::EphemeralTurn),
        )
        .expect("manifest");
        let mut selected_environment = environment();
        selected_environment
            .providers
            .insert(String::from("inherited-mock"));
        let record = data
            .select_session_child_style(SessionStyleChildSelectionDataRequest {
                environment: selected_environment,
                manifest,
                tool_groups: BTreeSet::from([String::from("filesystem")]),
                memory_access: SessionStyleChildMemoryAccess::None,
                inherited_provider: Some(String::from("inherited-mock")),
                max_iterations: None,
                max_steps: None,
                max_tokens: Some(2_000),
                max_cost_micros: Some(200_000),
                max_duration_ms: None,
            })
            .expect("child selection");

        assert_eq!(record.status, SessionStyleCatalogStatus::Available);
        let selections = record.selections.expect("compiled selections");
        assert_eq!(selections.max_tokens, 2_000);
        assert_eq!(selections.max_cost_micros, 200_000);
        assert_eq!(selections.memory_provider, "none");
        let compiled: agentmod_session_style_sdk::CompiledSessionStyle = serde_json::from_str(
            record
                .compiled_json
                .as_deref()
                .expect("compiled descriptor"),
        )
        .expect("compiled descriptor");
        assert_eq!(compiled.allowed_providers, ["inherited-mock"]);
        assert!(
            compiled
                .graph
                .nodes
                .iter()
                .filter(|node| node.provider.is_some())
                .all(|node| node.provider.as_deref() == Some("inherited-mock"))
        );
    }

    #[test]
    fn child_memory_access_maps_explicitly_to_every_sdk_variant() {
        assert_eq!(
            sdk_child_memory_access(SessionStyleChildMemoryAccess::None),
            agentmod_session_style_sdk::ChildMemoryAccess::None
        );
        assert_eq!(
            sdk_child_memory_access(SessionStyleChildMemoryAccess::ReadOnly),
            agentmod_session_style_sdk::ChildMemoryAccess::ReadOnly
        );
        assert_eq!(
            sdk_child_memory_access(SessionStyleChildMemoryAccess::ReadWrite),
            agentmod_session_style_sdk::ChildMemoryAccess::ReadWrite
        );
    }

    #[test]
    fn invalid_and_exact_duplicate_entries_are_reported() {
        let style = agentmod_session_style_sdk::to_toml(
            &agentmod_session_style_sdk::built_in_manifest_for_version(
                BuiltInStyle::PersistentChat,
                "1.1.0",
            )
            .expect("frozen persistent version"),
        )
        .expect("toml");
        let data = super::super::RuntimeData::new(MockDependency {
            discovery: DependencyStyleDiscovery {
                manifests: vec![
                    dependency_manifest("bad.toml", "not = [valid"),
                    dependency_manifest("one.toml", &style),
                    dependency_manifest("two.toml", &style),
                ],
                ..DependencyStyleDiscovery::default()
            },
            ..MockDependency::default()
        });
        let catalog = data
            .session_style_catalog(SessionStyleCatalogDataRequest {
                environment: environment(),
                user_style_root: None,
                project_style_root: None,
                plugin_style_roots: Vec::new(),
                cache_root: None,
            })
            .expect("catalog");
        assert!(catalog.records.iter().any(
            |record| record.id.is_none() && record.status == SessionStyleCatalogStatus::Invalid
        ));
        assert!(
            catalog
                .records
                .iter()
                .filter(|record| record.id.as_deref() == Some("persistent-chat")
                    && record.version.as_deref() == Some("1.1.0"))
                .all(|record| record
                    .diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code == "duplicate_style_identity"))
        );
    }

    #[test]
    fn disabled_marker_quarantines_all_versions_even_when_sources_conflict() {
        let style = agentmod_session_style_sdk::to_toml(
            &agentmod_session_style_sdk::built_in_manifest(BuiltInStyle::EphemeralTurn),
        )
        .expect("toml");
        let data = super::super::RuntimeData::new(MockDependency {
            discovery: DependencyStyleDiscovery {
                manifests: vec![dependency_manifest("duplicate.toml", &style)],
                disabled_markers: vec![DependencyStyleDisabledMarker {
                    style_id: String::from("ephemeral-turn"),
                    source_kind: DependencyStyleSourceKind::User,
                    source_locator: String::from("ephemeral-turn.disabled"),
                }],
                ..DependencyStyleDiscovery::default()
            },
            ..MockDependency::default()
        });
        let catalog = data
            .session_style_catalog(catalog_request())
            .expect("catalog");
        let records = catalog
            .records
            .iter()
            .filter(|record| record.id.as_deref() == Some("ephemeral-turn"))
            .collect::<Vec<_>>();

        assert_eq!(records.len(), 3);
        assert!(
            records
                .iter()
                .all(|record| record.status == SessionStyleCatalogStatus::Disabled)
        );
        assert!(
            records
                .iter()
                .filter(|record| record.version.as_deref() == Some("1.2.0"))
                .all(|record| record.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == DUPLICATE_STYLE_IDENTITY_DIAGNOSTIC_CODE
                }))
        );
    }

    #[test]
    fn different_style_versions_coexist() {
        let style = agentmod_session_style_sdk::to_toml(
            &agentmod_session_style_sdk::built_in_manifest_for_version(
                BuiltInStyle::PersistentChat,
                "1.1.0",
            )
            .expect("frozen persistent version"),
        )
        .expect("toml");
        let data = super::super::RuntimeData::new(MockDependency {
            discovery: DependencyStyleDiscovery {
                manifests: vec![dependency_manifest(
                    "next.toml",
                    &style.replacen("version = \"1.1.0\"", "version = \"1.1.1\"", 1),
                )],
                ..DependencyStyleDiscovery::default()
            },
            ..MockDependency::default()
        });
        let catalog = data
            .session_style_catalog(SessionStyleCatalogDataRequest {
                environment: environment(),
                user_style_root: None,
                project_style_root: None,
                plugin_style_roots: Vec::new(),
                cache_root: None,
            })
            .expect("catalog");
        assert!(catalog.records.iter().any(|record| {
            record.id.as_deref() == Some("persistent-chat")
                && record.version.as_deref() == Some("1.1.0")
                && record.status == SessionStyleCatalogStatus::Available
        }));
        assert!(catalog.records.iter().any(|record| {
            record.id.as_deref() == Some("persistent-chat")
                && record.version.as_deref() == Some("1.1.1")
                && record.status == SessionStyleCatalogStatus::Available
        }));
    }

    fn dependency_manifest(locator: &str, contents: &str) -> DependencyStyleManifestRecord {
        DependencyStyleManifestRecord {
            source_locator: locator.into(),
            source_kind: DependencyStyleSourceKind::User,
            format: DependencyStyleManifestFormat::Toml,
            bytes: contents.len() as u64,
            contents: contents.into(),
        }
    }
}
