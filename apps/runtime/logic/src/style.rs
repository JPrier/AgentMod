//! Session-style discovery, selection, compatibility, and immutable binding.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use agentmod_primitives::ContentHash;
use agentmod_runtime_data::style::{
    SessionStyleCatalogDataRequest, SessionStyleCatalogRecord, SessionStyleCatalogStatus,
    SessionStyleDataError, SessionStyleDataPort, SessionStyleDecisionCapability,
    SessionStyleDiagnostic as DataDiagnostic, SessionStyleEnvironment as DataEnvironment,
    SessionStyleManifestFormat as DataManifestFormat, SessionStyleSourceKind as DataSourceKind,
    SessionStyleValidationDataRequest,
};
use semver::Version;
use serde_json::Value;
use thiserror::Error;

use crate::session::{
    SessionCompactionConfiguration, SessionMemoryConfiguration, SessionPermissionDefaults,
    SessionStyleBinding, SessionStyleBudgets, SessionStyleSource,
};

/// Logic-owned style compilation and discovery environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleEnvironment {
    /// Runtime API semantic version.
    pub runtime_api_version: String,
    /// Validated activated plugin-set hash.
    pub plugin_set_hash: String,
    /// Optional user style root.
    pub user_style_root: Option<PathBuf>,
    /// Optional project-local style root.
    pub project_style_root: Option<PathBuf>,
    /// Activated plugin style roots.
    pub plugin_style_roots: Vec<PathBuf>,
    /// Optional persistent compiled-cache root.
    pub cache_root: Option<PathBuf>,
    /// Available runtime capabilities.
    pub capabilities: BTreeSet<String>,
    /// Available tool groups.
    pub tool_groups: BTreeMap<String, BTreeSet<String>>,
    /// Available provider IDs.
    pub providers: BTreeSet<String>,
    /// Available plugin IDs.
    pub plugins: BTreeSet<String>,
    /// Available memory provider IDs.
    pub memory_providers: BTreeSet<String>,
    /// Available compaction strategies.
    pub compaction_strategies: BTreeSet<String>,
    /// Runtime-supported interceptor decisions.
    pub supported_decisions: BTreeSet<StyleDecisionCapability>,
    /// Resolved content-addressed graph sources.
    pub graph_references: BTreeMap<String, String>,
}

/// Logic-owned interceptor decision capability.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StyleDecisionCapability {
    /// Continue unchanged.
    Continue,
    /// Replace a proposal.
    Replace,
    /// Reject a proposal.
    Reject,
    /// Require durable approval.
    RequireApproval,
    /// Defer through a continuation.
    Defer,
    /// Cancel execution.
    Cancel,
    /// Fork supported execution.
    Fork,
}

/// Logic-owned inline manifest format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleManifestFormat {
    /// TOML.
    Toml,
    /// JSON.
    Json,
}

/// Logic-owned style source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleSource {
    /// Runtime built-in.
    BuiltIn,
    /// User file.
    User,
    /// Project file.
    Project,
    /// Plugin package.
    Plugin,
    /// Caller-provided transient manifest.
    Inline,
}

/// Logic-owned style availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleAvailability {
    /// Eligible for selection.
    Available,
    /// Explicitly disabled.
    Disabled,
    /// Manifest is invalid.
    Invalid,
    /// Runtime environment is incompatible.
    Incompatible,
    /// Exact identity is supplied by more than one source.
    Conflict,
}

/// Logic-owned structured diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleDiagnostic {
    /// Stable diagnostic code.
    pub code: String,
    /// Manifest or catalog path.
    pub path: String,
    /// Safe explanation.
    pub message: String,
    /// Remediation.
    pub help: String,
}

/// Lightweight logic-owned catalog row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleSummary {
    /// Stable ID.
    pub id: String,
    /// Semantic version.
    pub version: String,
    /// Source.
    pub source: StyleSource,
    /// Availability.
    pub availability: StyleAvailability,
    /// Canonical manifest hash when known.
    pub content_hash: Option<String>,
    /// Compiled cache key when available.
    pub compiled_cache_key: Option<String>,
    /// Runtime capabilities required by the compiled style.
    pub required_capabilities: Vec<String>,
}

/// Complete logic-owned inspection result.
#[derive(Clone, Debug, PartialEq)]
pub struct StyleInspection {
    /// Summary.
    pub summary: StyleSummary,
    /// Safe source locator.
    pub source_locator: String,
    /// Canonical manifest.
    pub manifest: Value,
    /// Compiled descriptor when available.
    pub compiled: Option<Value>,
    /// Structured diagnostics.
    pub diagnostics: Vec<StyleDiagnostic>,
}

/// List command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStylesCommand {
    /// Explicit environment.
    pub environment: StyleEnvironment,
}

/// Inspect/resolve command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectStyleCommand {
    /// ID or exact `id@version`.
    pub selector: String,
    /// Explicit environment.
    pub environment: StyleEnvironment,
}

/// Inline validation/compilation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidateStyleCommand {
    /// Complete source.
    pub manifest: String,
    /// Source encoding.
    pub format: StyleManifestFormat,
    /// Explicit environment.
    pub environment: StyleEnvironment,
}

/// Resolved immutable selection returned to session creation/branch logic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedStyle {
    /// Session-owned immutable binding.
    pub binding: SessionStyleBinding,
}

/// Compatibility command for a persisted immutable session binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidateStyleBindingCommand {
    /// Binding reconstructed from canonical session history.
    pub binding: SessionStyleBinding,
    /// Current runtime environment.
    pub environment: StyleEnvironment,
}

/// Narrow session-style logic boundary.
pub trait SessionStyleLogicPort {
    /// Lists the live style catalog.
    ///
    /// # Errors
    ///
    /// Returns a catalog data error when discovery or compilation fails.
    fn list_styles(
        &self,
        command: ListStylesCommand,
    ) -> Result<Vec<StyleSummary>, SessionStyleLogicError>;

    /// Inspects one exact or highest-version style.
    ///
    /// # Errors
    ///
    /// Returns a selector, discovery, conflict, or compiled-data error.
    fn inspect_style(
        &self,
        command: InspectStyleCommand,
    ) -> Result<StyleInspection, SessionStyleLogicError>;

    /// Validates and compiles one transient manifest.
    ///
    /// # Errors
    ///
    /// Returns a validation data error when the manifest cannot be processed.
    fn validate_style(
        &self,
        command: ValidateStyleCommand,
    ) -> Result<StyleInspection, SessionStyleLogicError>;

    /// Resolves an available style into an immutable session binding.
    ///
    /// # Errors
    ///
    /// Returns an availability or compatibility error when selection fails.
    fn resolve_style(
        &self,
        command: InspectStyleCommand,
    ) -> Result<ResolvedStyle, SessionStyleLogicError>;

    /// Confirms that a persisted binding still resolves to the exact same
    /// compatible compiled style. No replacement or version fallback occurs.
    ///
    /// # Errors
    ///
    /// Returns an explicit incompatibility error when the exact binding is no
    /// longer available in the current runtime environment.
    fn validate_style_binding(
        &self,
        command: ValidateStyleBindingCommand,
    ) -> Result<(), SessionStyleLogicError>;
}

impl<D> SessionStyleLogicPort for super::RuntimeLogic<D>
where
    D: SessionStyleDataPort,
{
    fn list_styles(
        &self,
        command: ListStylesCommand,
    ) -> Result<Vec<StyleSummary>, SessionStyleLogicError> {
        let catalog = self
            .data
            .session_style_catalog(catalog_request(command.environment))
            .map_err(SessionStyleLogicError::Data)?;
        Ok(catalog.records.iter().map(summary).collect())
    }

    fn inspect_style(
        &self,
        command: InspectStyleCommand,
    ) -> Result<StyleInspection, SessionStyleLogicError> {
        let selector = parse_selector(&command.selector)?;
        let catalog = self
            .data
            .session_style_catalog(catalog_request(command.environment))
            .map_err(SessionStyleLogicError::Data)?;
        inspection(select_record(&catalog.records, &selector)?)
    }

    fn validate_style(
        &self,
        command: ValidateStyleCommand,
    ) -> Result<StyleInspection, SessionStyleLogicError> {
        if command.manifest.is_empty() {
            return Err(SessionStyleLogicError::EmptyManifest);
        }
        let record = self
            .data
            .validate_session_style(SessionStyleValidationDataRequest {
                environment: data_environment(&command.environment),
                manifest: command.manifest,
                format: match command.format {
                    StyleManifestFormat::Toml => DataManifestFormat::Toml,
                    StyleManifestFormat::Json => DataManifestFormat::Json,
                },
            })
            .map_err(SessionStyleLogicError::Data)?;
        inspection(&record)
    }

    fn resolve_style(
        &self,
        command: InspectStyleCommand,
    ) -> Result<ResolvedStyle, SessionStyleLogicError> {
        let environment = command.environment.clone();
        let selector = parse_selector(&command.selector)?;
        let catalog = self
            .data
            .session_style_catalog(catalog_request(command.environment))
            .map_err(SessionStyleLogicError::Data)?;
        let record = select_record(&catalog.records, &selector)?;
        if availability(record) != StyleAvailability::Available {
            return Err(SessionStyleLogicError::Unavailable {
                selector: selector.render(),
                status: format!("{:?}", availability(record)).to_ascii_lowercase(),
            });
        }
        Ok(ResolvedStyle {
            binding: binding(record, &environment)?,
        })
    }

    fn validate_style_binding(
        &self,
        command: ValidateStyleBindingCommand,
    ) -> Result<(), SessionStyleLogicError> {
        let selector = format!("{}@{}", command.binding.id, command.binding.version);
        let resolved = self.resolve_style(InspectStyleCommand {
            selector: selector.clone(),
            environment: command.environment,
        })?;
        if resolved.binding != command.binding {
            return Err(SessionStyleLogicError::BindingIncompatible {
                selector,
                reason: String::from(
                    "the persisted identity no longer matches the live compiled style",
                ),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StyleSelector {
    id: String,
    version: Option<Version>,
}

impl StyleSelector {
    fn render(&self) -> String {
        self.version.as_ref().map_or_else(
            || self.id.clone(),
            |version| format!("{}@{version}", self.id),
        )
    }
}

fn parse_selector(value: &str) -> Result<StyleSelector, SessionStyleLogicError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 256 {
        return Err(SessionStyleLogicError::InvalidSelector);
    }
    let (id, version) = value
        .rsplit_once('@')
        .map_or((value, None), |(id, version)| (id, Some(version)));
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(SessionStyleLogicError::InvalidSelector);
    }
    let version = version
        .map(Version::parse)
        .transpose()
        .map_err(|_| SessionStyleLogicError::InvalidSelector)?;
    Ok(StyleSelector {
        id: id.to_owned(),
        version,
    })
}

fn select_record<'a>(
    records: &'a [SessionStyleCatalogRecord],
    selector: &StyleSelector,
) -> Result<&'a SessionStyleCatalogRecord, SessionStyleLogicError> {
    let mut matches = records
        .iter()
        .filter(|record| record.id.as_deref() == Some(selector.id.as_str()))
        .filter_map(|record| {
            let version = record.version.as_deref()?.parse::<Version>().ok()?;
            if selector
                .version
                .as_ref()
                .is_none_or(|selected| *selected == version)
            {
                Some((version, record))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    matches.sort_by(|(left_version, left), (right_version, right)| {
        right_version
            .cmp(left_version)
            .then_with(|| source_rank(left.source.kind).cmp(&source_rank(right.source.kind)))
            .then_with(|| left.source.locator.cmp(&right.source.locator))
    });
    let Some((selected_version, selected)) = matches.first() else {
        return Err(SessionStyleLogicError::NotFound(selector.render()));
    };
    if matches
        .iter()
        .skip(1)
        .any(|(version, _)| version == selected_version)
    {
        return Err(SessionStyleLogicError::Conflict(selector.render()));
    }
    Ok(*selected)
}

const fn source_rank(source: DataSourceKind) -> u8 {
    match source {
        DataSourceKind::Project => 0,
        DataSourceKind::User => 1,
        DataSourceKind::Plugin => 2,
        DataSourceKind::BuiltIn => 3,
        DataSourceKind::Inline => 4,
    }
}

fn catalog_request(environment: StyleEnvironment) -> SessionStyleCatalogDataRequest {
    SessionStyleCatalogDataRequest {
        environment: data_environment(&environment),
        user_style_root: environment.user_style_root,
        project_style_root: environment.project_style_root,
        plugin_style_roots: environment.plugin_style_roots,
        cache_root: environment.cache_root,
    }
}

fn data_environment(environment: &StyleEnvironment) -> DataEnvironment {
    DataEnvironment {
        runtime_api_version: environment.runtime_api_version.clone(),
        plugin_set_hash: environment.plugin_set_hash.clone(),
        capabilities: environment.capabilities.clone(),
        tool_groups: environment.tool_groups.clone(),
        providers: environment.providers.clone(),
        plugins: environment.plugins.clone(),
        memory_providers: environment.memory_providers.clone(),
        compaction_strategies: environment.compaction_strategies.clone(),
        supported_decisions: environment
            .supported_decisions
            .iter()
            .copied()
            .map(data_decision)
            .collect(),
        graph_references: environment.graph_references.clone(),
    }
}

const fn data_decision(value: StyleDecisionCapability) -> SessionStyleDecisionCapability {
    match value {
        StyleDecisionCapability::Continue => SessionStyleDecisionCapability::Continue,
        StyleDecisionCapability::Replace => SessionStyleDecisionCapability::Replace,
        StyleDecisionCapability::Reject => SessionStyleDecisionCapability::Reject,
        StyleDecisionCapability::RequireApproval => SessionStyleDecisionCapability::RequireApproval,
        StyleDecisionCapability::Defer => SessionStyleDecisionCapability::Defer,
        StyleDecisionCapability::Cancel => SessionStyleDecisionCapability::Cancel,
        StyleDecisionCapability::Fork => SessionStyleDecisionCapability::Fork,
    }
}

fn summary(record: &SessionStyleCatalogRecord) -> StyleSummary {
    let compiled = record
        .compiled_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<Value>(value).ok());
    StyleSummary {
        id: record
            .id
            .clone()
            .unwrap_or_else(|| format!("<invalid:{}>", record.source.locator)),
        version: record.version.clone().unwrap_or_default(),
        source: source(record.source.kind),
        availability: availability(record),
        content_hash: record.manifest_hash.clone(),
        compiled_cache_key: record.cache_key.clone(),
        required_capabilities: string_array(compiled.as_ref(), "required_capabilities"),
    }
}

fn inspection(
    record: &SessionStyleCatalogRecord,
) -> Result<StyleInspection, SessionStyleLogicError> {
    let manifest = record
        .canonical_manifest_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| SessionStyleLogicError::InvalidData)?
        .unwrap_or(Value::Null);
    let compiled = record
        .compiled_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| SessionStyleLogicError::InvalidData)?;
    Ok(StyleInspection {
        summary: summary(record),
        source_locator: record.source.locator.clone(),
        manifest,
        compiled,
        diagnostics: record.diagnostics.iter().map(diagnostic).collect(),
    })
}

fn binding(
    record: &SessionStyleCatalogRecord,
    environment: &StyleEnvironment,
) -> Result<SessionStyleBinding, SessionStyleLogicError> {
    let manifest_json = record
        .canonical_manifest_json
        .clone()
        .ok_or(SessionStyleLogicError::InvalidData)?;
    let compiled_json = record
        .compiled_json
        .clone()
        .ok_or(SessionStyleLogicError::InvalidData)?;
    let compiled: Value =
        serde_json::from_str(&compiled_json).map_err(|_| SessionStyleLogicError::InvalidData)?;
    let memory = required_child(&compiled, "memory")?;
    let compaction = required_child(&compiled, "compaction")?;
    let budgets = required_child(&compiled, "budgets")?;
    let approvals = required_child(&compiled, "approvals")?;
    let cache_key = required_child(&compiled, "cache_key")?;
    Ok(SessionStyleBinding {
        id: required_string(record.id.as_deref())?,
        version: required_string(record.version.as_deref())?,
        content_hash: parse_hash(record.manifest_hash.as_deref())?,
        compiled_cache_key: parse_hash(record.cache_key.as_deref())?,
        compiled_style_hash: parse_hash(record.compiled_hash.as_deref())?,
        source: session_source(record.source.kind),
        source_locator: record.source.locator.clone(),
        plugin_set_hash: environment
            .plugin_set_hash
            .parse()
            .map_err(|_| SessionStyleLogicError::InvalidData)?,
        capability_set_hash: parse_hash(
            cache_key.get("capability_set_hash").and_then(Value::as_str),
        )?,
        runtime_api_version: record.runtime_api.clone(),
        configuration_json: manifest_json,
        compiled_style_json: compiled_json,
        memory: SessionMemoryConfiguration {
            provider: required_value_string(memory, "provider")?,
            scopes: string_array(Some(memory), "scopes"),
            retrieval_timing: required_value_string(memory, "retrieval_timing")?,
            query_json: serde_json::to_string(required_child(memory, "query")?)
                .map_err(|_| SessionStyleLogicError::InvalidData)?,
            max_items: value_u64(memory, "max_items")?
                .try_into()
                .map_err(|_| SessionStyleLogicError::InvalidData)?,
            max_injected_bytes: value_u64(memory, "max_injected_bytes")?,
            write_policy: required_value_string(memory, "write_policy")?,
            injection_location: required_value_string(memory, "injection_location")?,
        },
        compaction: SessionCompactionConfiguration {
            strategy: required_value_string(compaction, "strategy")?,
            trigger_tokens: compaction.get("trigger_tokens").and_then(Value::as_u64),
            reserved_context_tokens: value_u64(compaction, "reserved_context_tokens")?,
            max_provider_projection_tokens: value_u64(
                compaction,
                "max_provider_projection_tokens",
            )?,
            preserve_unresolved_tasks: value_bool(compaction, "preserve_unresolved_tasks")?,
            preserve_active_processes: value_bool(compaction, "preserve_active_processes")?,
            preservation_requirements: string_array(Some(compaction), "preservation_requirements"),
        },
        tool_groups: string_array(Some(&compiled), "allowed_tool_groups"),
        harness: String::from("native"),
        required_capabilities: string_array(Some(&compiled), "required_capabilities"),
        interceptor_order: string_array(Some(&compiled), "interceptor_order"),
        budgets: SessionStyleBudgets {
            max_iterations: value_u64(budgets, "max_iterations")?
                .try_into()
                .map_err(|_| SessionStyleLogicError::InvalidData)?,
            max_steps: value_u64(budgets, "max_steps")?,
            max_tokens: value_u64(budgets, "max_tokens")?,
            max_cost_micros: value_u64(budgets, "max_cost_micros")?,
            max_duration_ms: value_u64(budgets, "max_duration_ms")?,
        },
        permission_defaults: SessionPermissionDefaults {
            default: required_value_string(approvals, "default")?,
            groups: approvals
                .get("groups")
                .and_then(Value::as_object)
                .ok_or(SessionStyleLogicError::InvalidData)?
                .iter()
                .map(|(key, value)| {
                    value
                        .as_str()
                        .map(|value| (key.clone(), value.to_owned()))
                        .ok_or(SessionStyleLogicError::InvalidData)
                })
                .collect::<Result<_, _>>()?,
        },
        child_agent_policy_json: canonical_child_json(&compiled, "child_agents")?,
        retry_policy_json: canonical_child_json(&compiled, "retry")?,
        termination_policy_json: canonical_child_json(&compiled, "termination")?,
    })
}

fn required_child<'a>(value: &'a Value, key: &str) -> Result<&'a Value, SessionStyleLogicError> {
    value.get(key).ok_or(SessionStyleLogicError::InvalidData)
}

fn canonical_child_json(value: &Value, key: &str) -> Result<String, SessionStyleLogicError> {
    serde_json::to_string(required_child(value, key)?)
        .map_err(|_| SessionStyleLogicError::InvalidData)
}

fn availability(record: &SessionStyleCatalogRecord) -> StyleAvailability {
    if record
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "style_duplicate_identity")
    {
        return StyleAvailability::Conflict;
    }
    match record.status {
        SessionStyleCatalogStatus::Available => StyleAvailability::Available,
        SessionStyleCatalogStatus::Disabled => StyleAvailability::Disabled,
        SessionStyleCatalogStatus::Invalid => StyleAvailability::Invalid,
        SessionStyleCatalogStatus::Incompatible => StyleAvailability::Incompatible,
    }
}

const fn source(value: DataSourceKind) -> StyleSource {
    match value {
        DataSourceKind::BuiltIn => StyleSource::BuiltIn,
        DataSourceKind::User => StyleSource::User,
        DataSourceKind::Project => StyleSource::Project,
        DataSourceKind::Plugin => StyleSource::Plugin,
        DataSourceKind::Inline => StyleSource::Inline,
    }
}

const fn session_source(value: DataSourceKind) -> SessionStyleSource {
    match value {
        DataSourceKind::BuiltIn => SessionStyleSource::BuiltIn,
        DataSourceKind::User => SessionStyleSource::User,
        DataSourceKind::Project => SessionStyleSource::Project,
        DataSourceKind::Plugin => SessionStyleSource::Plugin,
        DataSourceKind::Inline => SessionStyleSource::Inline,
    }
}

fn diagnostic(value: &DataDiagnostic) -> StyleDiagnostic {
    StyleDiagnostic {
        code: value.code.clone(),
        path: value.path.clone(),
        message: value.message.clone(),
        help: value.help.clone(),
    }
}

fn required_string(value: Option<&str>) -> Result<String, SessionStyleLogicError> {
    value
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or(SessionStyleLogicError::InvalidData)
}

fn required_value_string(value: &Value, key: &str) -> Result<String, SessionStyleLogicError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(SessionStyleLogicError::InvalidData)
}

fn parse_hash(value: Option<&str>) -> Result<ContentHash, SessionStyleLogicError> {
    value
        .ok_or(SessionStyleLogicError::InvalidData)?
        .parse()
        .map_err(|_| SessionStyleLogicError::InvalidData)
}

fn value_u64(value: &Value, key: &str) -> Result<u64, SessionStyleLogicError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or(SessionStyleLogicError::InvalidData)
}

fn value_bool(value: &Value, key: &str) -> Result<bool, SessionStyleLogicError> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or(SessionStyleLogicError::InvalidData)
}

fn string_array(value: Option<&Value>, key: &str) -> Vec<String> {
    value
        .and_then(|value| value.get(key))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

/// Session-style business failure.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum SessionStyleLogicError {
    /// Catalog/compilation data failed.
    #[error("session-style catalog failed: {0}")]
    Data(SessionStyleDataError),
    /// Selector syntax is invalid.
    #[error("session-style selector is invalid")]
    InvalidSelector,
    /// No matching style exists.
    #[error("session style `{0}` was not found")]
    NotFound(String),
    /// More than one source owns the exact selected identity.
    #[error("session style `{0}` has conflicting sources")]
    Conflict(String),
    /// Selected style cannot be activated.
    #[error("session style `{selector}` is {status}")]
    Unavailable {
        /// Requested selector.
        selector: String,
        /// Availability.
        status: String,
    },
    /// Inline source was empty.
    #[error("session-style manifest is empty")]
    EmptyManifest,
    /// Data returned an internally inconsistent compiled descriptor.
    #[error("session-style catalog returned invalid compiled data")]
    InvalidData,
    /// A persisted session binding no longer matches the live catalog.
    #[error("session style `{selector}` is incompatible: {reason}")]
    BindingIncompatible {
        /// Exact persisted selector.
        selector: String,
        /// Stable safe incompatibility explanation.
        reason: String,
    },
}
