use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use agentmod_event_pipeline::{CompileDiagnostic, OrderingSpec, compile_order};
use semver::{Version, VersionReq};

use crate::{
    AuthorityTarget, ConfigurationSchemaSource, Entrypoint, FailurePolicy, IsolationMode,
    PluginCategory, PluginClassification, PluginManifest, PluginScope, TrustLevel,
};

/// Current supported plugin manifest schema.
pub const CURRENT_MANIFEST_SCHEMA_VERSION: u16 = 1;

/// Validation severity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DiagnosticSeverity {
    /// Activation-blocking validation error.
    Error,
}

/// Stable structured manifest diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    /// Stable machine code.
    pub code: &'static str,
    /// Manifest field path.
    pub path: String,
    /// Human-readable problem.
    pub message: String,
    /// Human-readable remediation.
    pub help: String,
    /// Severity.
    pub severity: DiagnosticSeverity,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "error[{}] at {}: {}\n  help: {}",
            self.code, self.path, self.message, self.help
        )
    }
}

/// Deterministically ordered validation diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationReport {
    diagnostics: Vec<Diagnostic>,
}

impl ValidationReport {
    /// Returns diagnostics in stable code/path/message order.
    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    /// Returns whether activation is prohibited.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.diagnostics.is_empty()
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, diagnostic) in self.diagnostics.iter().enumerate() {
            if index > 0 {
                formatter.write_str("\n")?;
            }
            diagnostic.fmt(formatter)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationReport {}

/// Runtime facts used to validate an activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationContext {
    /// Running semantic plugin API version.
    pub runtime_api_version: String,
    /// Capabilities available before loading the candidate plugin set.
    pub available_capabilities: Vec<String>,
    /// Runtime hard timeout ceiling.
    pub maximum_timeout_ms: u64,
}

impl ValidationContext {
    /// Creates a validation context with a five-minute timeout ceiling.
    #[must_use]
    pub fn new(runtime_api_version: impl Into<String>) -> Self {
        Self {
            runtime_api_version: runtime_api_version.into(),
            available_capabilities: Vec::new(),
            maximum_timeout_ms: 300_000,
        }
    }
}

/// Manifest proven valid for one validation context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedPlugin {
    manifest: PluginManifest,
}

impl ValidatedPlugin {
    /// Returns the validated owned manifest.
    #[must_use]
    pub fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    /// Consumes the wrapper and returns the owned manifest.
    #[must_use]
    pub fn into_manifest(self) -> PluginManifest {
        self.manifest
    }
}

/// Validates one manifest against runtime capabilities.
///
/// # Errors
///
/// Returns all deterministic intrinsic and compatibility diagnostics.
pub fn validate_manifest(
    manifest: &PluginManifest,
    context: &ValidationContext,
) -> Result<ValidatedPlugin, ValidationReport> {
    let available: BTreeSet<_> = context.available_capabilities.iter().cloned().collect();
    let mut diagnostics = validate_intrinsic(manifest, context, &available);
    sort_diagnostics(&mut diagnostics);
    if diagnostics.is_empty() {
        Ok(ValidatedPlugin {
            manifest: manifest.clone(),
        })
    } else {
        Err(ValidationReport { diagnostics })
    }
}

/// Validates a complete plugin set, including cross-plugin capabilities and
/// ordering dependencies.
///
/// # Errors
///
/// Returns intrinsic, duplicate-ID, missing-dependency, or cycle diagnostics.
pub fn validate_plugin_set(
    manifests: &[PluginManifest],
    context: &ValidationContext,
) -> Result<Vec<ValidatedPlugin>, ValidationReport> {
    let mut available: BTreeSet<_> = context.available_capabilities.iter().cloned().collect();
    for manifest in manifests {
        available.extend(manifest.provided_capabilities.iter().cloned());
    }
    let mut diagnostics = Vec::new();
    for manifest in manifests {
        diagnostics.extend(validate_intrinsic(manifest, context, &available));
    }

    let mut identities = BTreeMap::<&str, usize>::new();
    for manifest in manifests {
        *identities.entry(&manifest.identity.id).or_default() += 1;
    }
    for (plugin_id, count) in identities.into_iter().filter(|(_, count)| *count > 1) {
        diagnostics.push(error(
            "PLUG023",
            format!("plugins[{plugin_id}]"),
            format!("plugin ID is declared {count} times"),
            "plugin IDs must be unique in an activation set",
        ));
    }

    if diagnostics.is_empty() {
        let specifications: Vec<_> = manifests
            .iter()
            .map(|manifest| {
                let mut specification =
                    OrderingSpec::new(manifest.identity.id.as_str(), manifest.identity.id.as_str())
                        .with_stage(manifest.ordering.stage)
                        .with_priority(manifest.ordering.priority);
                for target in &manifest.ordering.before {
                    specification = specification.before(target.as_str());
                }
                for target in &manifest.ordering.after {
                    specification = specification.after(target.as_str());
                }
                specification
            })
            .collect();
        if let Err(ordering_error) = compile_order(&specifications) {
            for diagnostic in ordering_error.diagnostics() {
                diagnostics.push(map_ordering_diagnostic(diagnostic));
            }
        }
    }

    sort_diagnostics(&mut diagnostics);
    if diagnostics.is_empty() {
        Ok(manifests
            .iter()
            .cloned()
            .map(|manifest| ValidatedPlugin { manifest })
            .collect())
    } else {
        Err(ValidationReport { diagnostics })
    }
}

fn validate_intrinsic(
    manifest: &PluginManifest,
    context: &ValidationContext,
    available: &BTreeSet<String>,
) -> Vec<Diagnostic> {
    let plugin_path = format!("plugins[{}]", manifest.identity.id);
    let mut diagnostics = Vec::new();
    if manifest.schema_version != CURRENT_MANIFEST_SCHEMA_VERSION {
        diagnostics.push(error(
            "PLUG001",
            format!("{plugin_path}.schema_version"),
            format!("unsupported schema version {}", manifest.schema_version),
            format!("use schema version {CURRENT_MANIFEST_SCHEMA_VERSION}"),
        ));
    }
    if !is_identifier(&manifest.identity.id) {
        diagnostics.push(error(
            "PLUG002",
            format!("{plugin_path}.identity.id"),
            "plugin ID is not a safe lowercase identifier",
            "use 3-128 lowercase letters, digits, dots, underscores, or hyphens",
        ));
    }
    if Version::parse(&manifest.identity.version).is_err() {
        diagnostics.push(error(
            "PLUG003",
            format!("{plugin_path}.identity.version"),
            "plugin version is not semantic versioning",
            "declare a complete semantic version such as 1.2.3",
        ));
    }
    validate_runtime_api(manifest, context, &plugin_path, &mut diagnostics);
    validate_category(manifest, &plugin_path, &mut diagnostics);
    validate_entrypoint(manifest, &plugin_path, &mut diagnostics);
    validate_timeout_failure(manifest, context, &plugin_path, &mut diagnostics);
    validate_capabilities(manifest, available, &plugin_path, &mut diagnostics);
    validate_subscriptions(manifest, &plugin_path, &mut diagnostics);
    validate_authorities(manifest, &plugin_path, &mut diagnostics);
    validate_permissions(manifest, &plugin_path, &mut diagnostics);
    validate_ordering(manifest, &plugin_path, &mut diagnostics);
    validate_configuration(manifest, &plugin_path, &mut diagnostics);
    if manifest.state_migration_version == 0 {
        diagnostics.push(error(
            "PLUG019",
            format!("{plugin_path}.state_migration_version"),
            "state migration version must be positive",
            "start plugin state migrations at version 1",
        ));
    }
    diagnostics
}

fn validate_runtime_api(
    manifest: &PluginManifest,
    context: &ValidationContext,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let requirement = VersionReq::parse(&manifest.identity.runtime_api);
    let runtime_version = Version::parse(&context.runtime_api_version);
    match (requirement, runtime_version) {
        (Err(_), _) => diagnostics.push(error(
            "PLUG004",
            format!("{plugin_path}.identity.runtime_api"),
            "runtime API requirement is not a semantic version requirement",
            "use a requirement such as ^1.2",
        )),
        (_, Err(_)) => diagnostics.push(error(
            "PLUG004",
            "$context.runtime_api_version",
            "runtime API version is not semantic versioning",
            "supply a complete semantic version",
        )),
        (Ok(requirement), Ok(runtime)) if !requirement.matches(&runtime) => {
            diagnostics.push(error(
                "PLUG005",
                format!("{plugin_path}.identity.runtime_api"),
                format!(
                    "runtime API {} does not satisfy {}",
                    context.runtime_api_version, manifest.identity.runtime_api
                ),
                "install a compatible plugin or runtime",
            ));
        }
        (Ok(_), Ok(_)) => {}
    }
}

fn validate_category(
    manifest: &PluginManifest,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let compatible = !matches!(
        (manifest.category, manifest.classification),
        (PluginCategory::Interceptor, PluginClassification::Observer)
            | (PluginCategory::Observer, PluginClassification::Blocking)
    );
    if !compatible {
        diagnostics.push(error(
            "PLUG006",
            format!("{plugin_path}.classification"),
            "category conflicts with blocking/observer classification",
            "interceptors are blocking and observer-category plugins are observers",
        ));
    }
}

fn validate_entrypoint(
    manifest: &PluginManifest,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let entrypoint_valid = match (&manifest.entrypoint, manifest.isolation) {
        (Entrypoint::RustBuiltin { symbol }, IsolationMode::TrustedInProcess) => is_symbol(symbol),
        (Entrypoint::Process { program, args }, IsolationMode::Process) => {
            !program.trim().is_empty()
                && program.len() <= 512
                && !program.contains('\0')
                && args.len() <= 64
                && args
                    .iter()
                    .all(|argument| argument.len() <= 4096 && !argument.contains('\0'))
        }
        (Entrypoint::WasiComponent { component }, IsolationMode::Wasi) => {
            is_safe_relative_path(component)
        }
        _ => false,
    };
    if !entrypoint_valid {
        diagnostics.push(error(
            "PLUG007",
            format!("{plugin_path}.entrypoint"),
            "entrypoint is empty, unsafe, oversized, or mismatched with isolation",
            "use rust_builtin/trusted_in_process, process/process, or wasi_component/wasi",
        ));
    }
    if manifest.isolation == IsolationMode::TrustedInProcess
        && manifest.trust != TrustLevel::FirstParty
    {
        diagnostics.push(error(
            "PLUG008",
            format!("{plugin_path}.trust"),
            "non-first-party plugins cannot run in process",
            "use process or WASI isolation for third-party plugins",
        ));
    }
    if manifest.trust == TrustLevel::Untrusted {
        diagnostics.push(error(
            "PLUG008",
            format!("{plugin_path}.trust"),
            "untrusted plugins cannot be activated",
            "approve, sandbox, or keep the plugin disabled",
        ));
    }
}

fn validate_timeout_failure(
    manifest: &PluginManifest,
    context: &ValidationContext,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let maximum = context.maximum_timeout_ms.min(300_000);
    if manifest.timeout_ms == 0 || manifest.timeout_ms > maximum {
        diagnostics.push(error(
            "PLUG009",
            format!("{plugin_path}.timeout_ms"),
            format!("timeout must be between 1 and {maximum} milliseconds"),
            "choose a bounded handler timeout",
        ));
    }
    let compatible = match (&manifest.failure_policy, manifest.classification) {
        (FailurePolicy::Continue, PluginClassification::Blocking)
        | (FailurePolicy::Reject | FailurePolicy::Cancel, PluginClassification::Observer) => false,
        (
            FailurePolicy::Retry {
                max_attempts,
                backoff_ms,
            },
            _,
        ) => {
            (1..=10).contains(max_attempts)
                && *backoff_ms < manifest.timeout_ms
                && *backoff_ms <= maximum
        }
        _ => true,
    };
    if !compatible {
        diagnostics.push(error(
            "PLUG010",
            format!("{plugin_path}.failure_policy"),
            "failure policy is incompatible with classification, attempts, or timeout",
            "observers may continue; blockers may reject/cancel; retries require 1-10 attempts and backoff below timeout",
        ));
    }
}

fn validate_capabilities(
    manifest: &PluginManifest,
    available: &BTreeSet<String>,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if manifest.required_capabilities.len() > 128 || manifest.provided_capabilities.len() > 128 {
        diagnostics.push(bounds(plugin_path, "capabilities"));
    }
    validate_unique_strings(
        &manifest.required_capabilities,
        "PLUG011",
        &format!("{plugin_path}.required_capabilities"),
        "required capability",
        diagnostics,
    );
    validate_unique_strings(
        &manifest.provided_capabilities,
        "PLUG011",
        &format!("{plugin_path}.provided_capabilities"),
        "provided capability",
        diagnostics,
    );
    let provided: BTreeSet<_> = manifest.provided_capabilities.iter().collect();
    for (index, capability) in manifest.required_capabilities.iter().enumerate() {
        if !is_capability(capability) {
            diagnostics.push(error(
                "PLUG013",
                format!("{plugin_path}.required_capabilities[{index}]"),
                "capability identifier is invalid",
                "use a lowercase dotted or colon-delimited capability name",
            ));
        }
        if provided.contains(capability) {
            diagnostics.push(error(
                "PLUG013",
                format!("{plugin_path}.required_capabilities[{index}]"),
                "capability is both required and provided by the same plugin",
                "remove the circular self-requirement",
            ));
        }
        if !available.contains(capability) {
            diagnostics.push(error(
                "PLUG012",
                format!("{plugin_path}.required_capabilities[{index}]"),
                format!("required capability `{capability}` is unavailable"),
                "install/activate a provider or remove the requirement",
            ));
        }
    }
    for (index, capability) in manifest.provided_capabilities.iter().enumerate() {
        if !is_capability(capability) {
            diagnostics.push(error(
                "PLUG013",
                format!("{plugin_path}.provided_capabilities[{index}]"),
                "capability identifier is invalid",
                "use a lowercase dotted or colon-delimited capability name",
            ));
        }
    }
}

fn validate_subscriptions(
    manifest: &PluginManifest,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if manifest.subscribed_events.len() > 256 {
        diagnostics.push(bounds(plugin_path, "subscribed_events"));
    }
    validate_unique_strings(
        &manifest.subscribed_events,
        "PLUG014",
        &format!("{plugin_path}.subscribed_events"),
        "event subscription",
        diagnostics,
    );
    for (index, event) in manifest.subscribed_events.iter().enumerate() {
        if !is_event(event) {
            diagnostics.push(error(
                "PLUG014",
                format!("{plugin_path}.subscribed_events[{index}]"),
                "event subscription name is invalid",
                "use a lowercase dotted event name",
            ));
        }
    }
    if matches!(
        manifest.category,
        PluginCategory::Interceptor | PluginCategory::Observer
    ) && manifest.subscribed_events.is_empty()
    {
        diagnostics.push(error(
            "PLUG014",
            format!("{plugin_path}.subscribed_events"),
            "interceptor/observer category requires at least one event",
            "declare the proposal or committed event handled by the plugin",
        ));
    }
}

fn validate_authorities(
    manifest: &PluginManifest,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    validate_unique_values(
        &manifest.authorities.read,
        &format!("{plugin_path}.authorities.read"),
        diagnostics,
    );
    validate_unique_values(
        &manifest.authorities.proposed_write,
        &format!("{plugin_path}.authorities.proposed_write"),
        diagnostics,
    );
    for (index, target) in manifest.authorities.read.iter().enumerate() {
        if *target == AuthorityTarget::ExternalNotification
            || authority_rank(*target).is_some_and(|rank| rank > scope_rank(manifest.scope))
        {
            diagnostics.push(error(
                "PLUG015",
                format!("{plugin_path}.authorities.read[{index}]"),
                "read authority is illegal for the plugin scope or is write-only",
                "reduce the authority target or broaden the declared plugin scope",
            ));
        }
    }
    for (index, target) in manifest.authorities.proposed_write.iter().enumerate() {
        if manifest.classification == PluginClassification::Observer
            && *target == AuthorityTarget::CanonicalState
        {
            diagnostics.push(error(
                "PLUG016",
                format!("{plugin_path}.authorities.proposed_write[{index}]"),
                "observers cannot request canonical state write authority",
                "use a blocking interceptor to propose canonical changes",
            ));
            continue;
        }
        let observer_allowed = matches!(
            target,
            AuthorityTarget::DerivedIndex
                | AuthorityTarget::PluginState
                | AuthorityTarget::ExternalNotification
        );
        let exceeds_scope =
            authority_rank(*target).is_some_and(|rank| rank > scope_rank(manifest.scope));
        if (manifest.classification == PluginClassification::Observer && !observer_allowed)
            || exceeds_scope
        {
            diagnostics.push(error(
                "PLUG015",
                format!("{plugin_path}.authorities.proposed_write[{index}]"),
                "proposed write authority is illegal for classification or scope",
                "observers may update only derived/plugin/notification state; other writes must remain within scope",
            ));
        }
    }
}

fn validate_permissions(
    manifest: &PluginManifest,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    validate_unique_strings(
        &manifest.permissions.tools,
        "PLUG017",
        &format!("{plugin_path}.permissions.tools"),
        "tool permission",
        diagnostics,
    );
    validate_unique_strings(
        &manifest.permissions.network,
        "PLUG017",
        &format!("{plugin_path}.permissions.network"),
        "network permission",
        diagnostics,
    );
    for (index, tool) in manifest.permissions.tools.iter().enumerate() {
        if !is_capability(tool) {
            diagnostics.push(error(
                "PLUG017",
                format!("{plugin_path}.permissions.tools[{index}]"),
                "tool permission is invalid",
                "use a stable lowercase tool or group name",
            ));
        }
    }
    for (index, domain) in manifest.permissions.network.iter().enumerate() {
        if !is_domain(domain) {
            diagnostics.push(error(
                "PLUG017",
                format!("{plugin_path}.permissions.network[{index}]"),
                "network permission is not an exact domain or safe subdomain pattern",
                "use example.com or *.example.com without scheme, port, or path",
            ));
        }
    }
}

fn validate_ordering(
    manifest: &PluginManifest,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    validate_unique_strings(
        &manifest.ordering.before,
        "PLUG020",
        &format!("{plugin_path}.ordering.before"),
        "before dependency",
        diagnostics,
    );
    validate_unique_strings(
        &manifest.ordering.after,
        "PLUG020",
        &format!("{plugin_path}.ordering.after"),
        "after dependency",
        diagnostics,
    );
    for (field, values) in [
        ("before", &manifest.ordering.before),
        ("after", &manifest.ordering.after),
    ] {
        for (index, target) in values.iter().enumerate() {
            if target == &manifest.identity.id || !is_identifier(target) {
                diagnostics.push(error(
                    "PLUG020",
                    format!("{plugin_path}.ordering.{field}[{index}]"),
                    "ordering target is self-referential or invalid",
                    "reference a different valid plugin ID",
                ));
            }
        }
    }
}

fn validate_configuration(
    manifest: &PluginManifest,
    plugin_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if !is_capability(&manifest.configuration.schema_id)
        || manifest.configuration.schema_version == 0
    {
        diagnostics.push(error(
            "PLUG018",
            format!("{plugin_path}.configuration"),
            "configuration schema ID or version is invalid",
            "use a stable schema ID and positive version",
        ));
    }
    let source_valid = match &manifest.configuration.source {
        ConfigurationSchemaSource::InlineJson { document } => {
            document.len() <= 65_536
                && serde_json::from_str::<serde_json::Value>(document)
                    .is_ok_and(|value| value.is_object())
        }
        ConfigurationSchemaSource::File { relative_path } => {
            is_safe_relative_path(relative_path)
                && relative_path
                    .rsplit_once('.')
                    .is_some_and(|(_, extension)| extension == "json")
        }
    };
    if !source_valid {
        diagnostics.push(error(
            "PLUG018",
            format!("{plugin_path}.configuration.source"),
            "configuration schema source is invalid, oversized, or unsafe",
            "supply an inline JSON object or safe relative .json path",
        ));
    }
}

fn map_ordering_diagnostic(diagnostic: &CompileDiagnostic) -> Diagnostic {
    match diagnostic {
        CompileDiagnostic::MissingDependency {
            handler,
            missing,
            relation,
        } => error(
            "PLUG021",
            format!("plugins[{handler}].ordering.{relation}"),
            format!("ordering references missing plugin `{missing}`"),
            "activate the referenced plugin or remove the ordering constraint",
        ),
        CompileDiagnostic::OrderingCycle { path } => error(
            "PLUG022",
            "plugins.ordering",
            format!(
                "plugin ordering contains cycle: {}",
                path.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ),
            "remove or reverse a before/after constraint",
        ),
        CompileDiagnostic::DuplicateHandler { handler, .. } => error(
            "PLUG023",
            format!("plugins[{handler}]"),
            "plugin ID is duplicated",
            "plugin IDs must be unique",
        ),
    }
}

fn validate_unique_strings(
    values: &[String],
    code: &'static str,
    path: &str,
    label: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut seen = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        if !seen.insert(value) {
            diagnostics.push(error(
                code,
                format!("{path}[{index}]"),
                format!("duplicate {label} `{value}`"),
                "remove the duplicate declaration",
            ));
        }
    }
}

fn validate_unique_values(
    values: &[AuthorityTarget],
    path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut seen = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        if !seen.insert(value) {
            diagnostics.push(error(
                "PLUG011",
                format!("{path}[{index}]"),
                format!("duplicate authority `{value:?}`"),
                "remove the duplicate authority",
            ));
        }
    }
}

fn authority_rank(target: AuthorityTarget) -> Option<u8> {
    match target {
        AuthorityTarget::InvocationState => Some(0),
        AuthorityTarget::ModelCallState => Some(1),
        AuthorityTarget::TurnState => Some(2),
        AuthorityTarget::SessionState | AuthorityTarget::CanonicalState => Some(3),
        AuthorityTarget::ProjectState => Some(4),
        AuthorityTarget::UserState => Some(5),
        AuthorityTarget::RuntimeState => Some(6),
        AuthorityTarget::DerivedIndex
        | AuthorityTarget::PluginState
        | AuthorityTarget::ExternalNotification => None,
    }
}

const fn scope_rank(scope: PluginScope) -> u8 {
    match scope {
        PluginScope::Invocation => 0,
        PluginScope::ModelCall => 1,
        PluginScope::Turn => 2,
        PluginScope::Session => 3,
        PluginScope::Project => 4,
        PluginScope::User => 5,
        PluginScope::Runtime => 6,
    }
}

fn is_identifier(value: &str) -> bool {
    (3..=128).contains(&value.len())
        && !value.starts_with(['.', '-', '_'])
        && !value.ends_with(['.', '-', '_'])
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-_".contains(&byte)
        })
}

fn is_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
}

fn is_capability(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.starts_with(['.', ':', '-', '_'])
        && !value.ends_with(['.', ':', '-', '_'])
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".:_-".contains(&byte)
        })
}

fn is_event(value: &str) -> bool {
    is_capability(value) && value.contains('.')
}

fn is_domain(value: &str) -> bool {
    let domain = value.strip_prefix("*.").unwrap_or(value);
    !domain.is_empty()
        && domain.len() <= 253
        && domain.contains('.')
        && domain.split('.').all(|label| {
            !label.is_empty()
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn is_safe_relative_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !value.starts_with(['/', '\\'])
        && !value.contains('\0')
        && !value.contains('\\')
        && value
            .split('/')
            .all(|component| !component.is_empty() && !matches!(component, "." | ".."))
}

fn bounds(plugin_path: &str, field: &str) -> Diagnostic {
    error(
        "PLUG024",
        format!("{plugin_path}.{field}"),
        "manifest collection exceeds its deterministic bound",
        "reduce the number of declarations",
    )
}

fn error(
    code: &'static str,
    path: impl Into<String>,
    message: impl Into<String>,
    help: impl Into<String>,
) -> Diagnostic {
    Diagnostic {
        code,
        path: path.into(),
        message: message.into(),
        help: help.into(),
        severity: DiagnosticSeverity::Error,
    }
}

fn sort_diagnostics(diagnostics: &mut [Diagnostic]) {
    diagnostics.sort_by(|left, right| {
        left.code
            .cmp(right.code)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.message.cmp(&right.message))
    });
}
