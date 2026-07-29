use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use agentmod_event_pipeline::{CompileDiagnostic, OrderingSpec, compile_order};
use agentmod_graph_engine::{
    CompilerLimits, ExecutableGraph, GraphCacheInputs, GraphDefinition, GraphError, NodeKind,
    compile as compile_graph,
};
use agentmod_primitives::ContentHash;
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};

use crate::{
    ApprovalDefaults, BuiltInStyle, ChildAgentLimits, ChildWorkspaceMode, CompactionSelection,
    CompactionStrategy, DecisionCapability, ExecutionBudgets, GraphSource, InterceptorDeclaration,
    MemoryInjectionLocation, MemoryRetrievalTiming, MemorySelection, MemoryWritePolicy,
    RetryPolicy, SessionStyleManifest, StyleKind, TerminationOutcome, TerminationPolicy,
    TopLevelSelection,
};

/// Current session-style manifest schema.
pub const CURRENT_STYLE_SCHEMA_VERSION: u16 = 1;

/// Hard compiler limits for untrusted style manifests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StyleCompilerLimits {
    /// Maximum manifest collections.
    pub max_collection_items: usize,
    /// Maximum interceptor registrations.
    pub max_interceptors: usize,
    /// Maximum memory records injected.
    pub max_memory_items: u32,
    /// Maximum injected memory bytes.
    pub max_memory_bytes: u64,
    /// Maximum style iterations.
    pub max_iterations: u32,
    /// Maximum graph steps.
    pub max_steps: u64,
    /// Maximum tokens.
    pub max_tokens: u64,
    /// Maximum cost micros.
    pub max_cost_micros: u64,
    /// Maximum duration.
    pub max_duration_ms: u64,
    /// Maximum children.
    pub max_children: u32,
    /// Maximum concurrent children.
    pub max_concurrent_children: u32,
    /// Maximum recursive child depth.
    pub max_child_depth: u16,
    /// Maximum retry attempts.
    pub max_retry_attempts: u32,
    /// Generic graph compiler limits.
    pub graph: CompilerLimits,
}

impl Default for StyleCompilerLimits {
    fn default() -> Self {
        Self {
            max_collection_items: 256,
            max_interceptors: 256,
            max_memory_items: 1_024,
            max_memory_bytes: 16 * 1024 * 1024,
            max_iterations: 10_000,
            max_steps: 10_000_000,
            max_tokens: 10_000_000_000,
            max_cost_micros: 1_000_000_000_000,
            max_duration_ms: 365 * 24 * 60 * 60 * 1_000,
            max_children: 1_024,
            max_concurrent_children: 128,
            max_child_depth: 32,
            max_retry_attempts: 32,
            graph: CompilerLimits::default(),
        }
    }
}

/// Runtime and registry inputs used for deterministic validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileContext {
    /// Running runtime API semantic version.
    pub runtime_api_version: String,
    /// Validated plugin-set hash.
    pub plugin_set_hash: ContentHash,
    /// Available runtime capabilities.
    pub capabilities: BTreeSet<String>,
    /// Available tool groups and their exact tool IDs.
    pub tool_groups: BTreeMap<String, BTreeSet<String>>,
    /// Available provider IDs.
    pub providers: BTreeSet<String>,
    /// Available plugin IDs.
    pub plugins: BTreeSet<String>,
    /// Available memory provider IDs.
    pub memory_providers: BTreeSet<String>,
    /// Available compaction strategy IDs.
    pub compaction_strategies: BTreeSet<String>,
    /// Decision kinds supported by the action/runtime API.
    pub supported_decisions: BTreeSet<DecisionCapability>,
    /// Content-addressed graph references supplied without SDK I/O.
    pub graph_references: BTreeMap<String, String>,
}

/// Validation severity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    /// Activation-blocking error.
    Error,
}

/// Stable structured style diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Diagnostic {
    /// Stable code.
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

/// Deterministically sorted compilation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleCompileError {
    diagnostics: Vec<Diagnostic>,
}

impl StyleCompileError {
    /// Returns diagnostics sorted by code, path, and message.
    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }
}

impl fmt::Display for StyleCompileError {
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

impl std::error::Error for StyleCompileError {}

/// Complete cache identity for a compiled style.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StyleCacheKey {
    /// Canonical manifest content hash.
    pub style_content_hash: ContentHash,
    /// Validated plugin-set hash.
    pub plugin_set_hash: ContentHash,
    /// Runtime API version hash.
    pub runtime_api_hash: ContentHash,
    /// Sorted capability-set hash.
    pub capability_set_hash: ContentHash,
    /// Hash binding every constituent.
    pub combined_hash: ContentHash,
}

/// Inspectable compiled style descriptor; runtime logic interprets it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledSessionStyle {
    /// Stable style ID.
    pub style_id: String,
    /// Style semantic version.
    pub style_version: String,
    /// Built-in or custom provenance.
    pub kind: StyleKind,
    /// Built-in semantic, when applicable.
    pub built_in_semantic: Option<BuiltInStyle>,
    /// Deterministically compiled graph.
    pub graph: ExecutableGraph,
    /// Interceptor IDs in execution order.
    pub interceptor_order: Vec<String>,
    /// Runtime capabilities required by the style.
    pub required_capabilities: Vec<String>,
    /// Allowed tool groups.
    pub allowed_tool_groups: Vec<String>,
    /// Allowed providers.
    pub allowed_providers: Vec<String>,
    /// Allowed plugins.
    pub allowed_plugins: Vec<String>,
    /// Memory selection.
    pub memory: MemorySelection,
    /// Compaction selection.
    pub compaction: CompactionSelection,
    /// Approval defaults.
    pub approvals: ApprovalDefaults,
    /// Hard execution budgets.
    pub budgets: ExecutionBudgets,
    /// Child-agent bounds.
    pub child_agents: ChildAgentLimits,
    /// Retry policy.
    pub retry: RetryPolicy,
    /// Explicit termination policy.
    pub termination: TerminationPolicy,
    /// Top-level selection policy.
    pub selection: TopLevelSelection,
    /// Compatibility-bound cache identity.
    pub cache_key: StyleCacheKey,
}

impl CompiledSessionStyle {
    /// Returns deterministic pretty JSON for inspection.
    ///
    /// # Errors
    ///
    /// Returns an owned diagnostic if serialization unexpectedly fails.
    pub fn inspect_json(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }
}

/// Validates and compiles a style without performing external I/O.
///
/// # Errors
///
/// Returns every deterministic manifest/availability diagnostic available
/// before graph compilation, plus graph and interceptor compiler diagnostics.
pub fn compile_style(
    manifest: &SessionStyleManifest,
    context: &CompileContext,
    limits: StyleCompilerLimits,
) -> Result<CompiledSessionStyle, StyleCompileError> {
    let root = format!("styles[{}]", manifest.identity.id);
    let mut diagnostics = Vec::new();
    validate_identity(manifest, context, &root, &mut diagnostics);
    validate_kind(manifest, &root, &mut diagnostics);
    validate_collections(manifest, limits, &root, &mut diagnostics);
    validate_availability(manifest, context, &root, &mut diagnostics);
    validate_interceptors(manifest, context, limits, &root, &mut diagnostics);
    validate_memory(&manifest.memory, context, limits, &root, &mut diagnostics);
    validate_compaction(
        &manifest.compaction,
        context,
        manifest.budgets.max_tokens,
        &root,
        &mut diagnostics,
    );
    validate_approvals(&manifest.approvals, &root, &mut diagnostics);
    validate_budgets(manifest.budgets, limits, &root, &mut diagnostics);
    validate_children(
        &manifest.child_agents,
        manifest.budgets,
        &manifest.required_capabilities,
        &manifest.allowed_tool_groups,
        limits,
        &root,
        &mut diagnostics,
    );
    validate_retry(
        &manifest.retry,
        manifest.budgets,
        limits,
        &root,
        &mut diagnostics,
    );
    validate_termination(&manifest.termination, &root, &mut diagnostics);
    validate_selection(manifest.selection, &root, &mut diagnostics);

    let graph_source = resolve_graph(manifest, context, &root, &mut diagnostics);
    let interceptor_order = compile_interceptors(manifest, &root, &mut diagnostics);
    let graph = graph_source.as_deref().and_then(|source| {
        compile_and_validate_graph(source, manifest, context, limits, &root, &mut diagnostics)
    });

    sort_diagnostics(&mut diagnostics);
    if !diagnostics.is_empty() {
        return Err(StyleCompileError { diagnostics });
    }
    let (Some(graph), Some(interceptor_order)) = (graph, interceptor_order) else {
        return Err(internal_compilation_error(&root));
    };
    let cache_key =
        build_cache_key(manifest, context).map_err(|_| internal_compilation_error(&root))?;
    Ok(CompiledSessionStyle {
        style_id: manifest.identity.id.clone(),
        style_version: manifest.identity.version.clone(),
        kind: manifest.kind,
        built_in_semantic: manifest.built_in_semantic,
        graph,
        interceptor_order,
        required_capabilities: sorted(&manifest.required_capabilities),
        allowed_tool_groups: sorted(&manifest.allowed_tool_groups),
        allowed_providers: sorted(&manifest.allowed_providers),
        allowed_plugins: sorted(&manifest.allowed_plugins),
        memory: manifest.memory.clone(),
        compaction: manifest.compaction.clone(),
        approvals: manifest.approvals.clone(),
        budgets: manifest.budgets,
        child_agents: manifest.child_agents.clone(),
        retry: manifest.retry.clone(),
        termination: manifest.termination.clone(),
        selection: manifest.selection,
        cache_key,
    })
}

/// Validates and compiles a catalog while enforcing unique style IDs.
///
/// # Errors
///
/// Returns deterministic diagnostics from every style plus duplicate catalog
/// identity diagnostics.
pub fn compile_style_set(
    manifests: &[SessionStyleManifest],
    context: &CompileContext,
    limits: StyleCompilerLimits,
) -> Result<Vec<CompiledSessionStyle>, StyleCompileError> {
    let mut diagnostics = Vec::new();
    let mut counts = BTreeMap::<&str, usize>::new();
    for manifest in manifests {
        *counts.entry(&manifest.identity.id).or_default() += 1;
    }
    for (id, count) in counts.into_iter().filter(|(_, count)| *count > 1) {
        diagnostics.push(error(
            "STYLE008",
            format!("styles[{id}]"),
            format!("style ID is declared {count} times"),
            "style IDs must be unique in a catalog",
        ));
    }

    let mut compiled = Vec::with_capacity(manifests.len());
    for manifest in manifests {
        match compile_style(manifest, context, limits) {
            Ok(style) => compiled.push(style),
            Err(error) => diagnostics.extend_from_slice(error.diagnostics()),
        }
    }
    sort_diagnostics(&mut diagnostics);
    if diagnostics.is_empty() {
        Ok(compiled)
    } else {
        Err(StyleCompileError { diagnostics })
    }
}

fn internal_compilation_error(root: &str) -> StyleCompileError {
    StyleCompileError {
        diagnostics: vec![error(
            "STYLE029",
            root,
            "validated style could not produce an owned compiled descriptor",
            "report this deterministic compiler invariant failure",
        )],
    }
}

fn validate_identity(
    manifest: &SessionStyleManifest,
    context: &CompileContext,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if manifest.schema_version != CURRENT_STYLE_SCHEMA_VERSION {
        diagnostics.push(error(
            "STYLE001",
            format!("{root}.schema_version"),
            format!("unsupported schema version {}", manifest.schema_version),
            format!("use schema version {CURRENT_STYLE_SCHEMA_VERSION}"),
        ));
    }
    if !is_name(&manifest.identity.id) {
        diagnostics.push(error(
            "STYLE002",
            format!("{root}.identity.id"),
            "style ID is invalid",
            "use 3-128 lowercase name bytes",
        ));
    }
    if Version::parse(&manifest.identity.version).is_err() {
        diagnostics.push(error(
            "STYLE003",
            format!("{root}.identity.version"),
            "style version is not semantic versioning",
            "use a complete semantic version such as 1.0.0",
        ));
    }
    match (
        VersionReq::parse(&manifest.identity.runtime_api),
        Version::parse(&context.runtime_api_version),
    ) {
        (Err(_), _) => diagnostics.push(error(
            "STYLE004",
            format!("{root}.identity.runtime_api"),
            "runtime API range is invalid",
            "use a semantic requirement such as ^1.0",
        )),
        (_, Err(_)) => diagnostics.push(error(
            "STYLE004",
            "$context.runtime_api_version",
            "runtime API version is invalid",
            "supply a complete semantic version",
        )),
        (Ok(requirement), Ok(version)) if !requirement.matches(&version) => {
            diagnostics.push(error(
                "STYLE005",
                format!("{root}.identity.runtime_api"),
                format!(
                    "runtime API {} does not satisfy {}",
                    context.runtime_api_version, manifest.identity.runtime_api
                ),
                "select a compatible style or runtime",
            ));
        }
        (Ok(_), Ok(_)) => {}
    }
}

fn validate_kind(manifest: &SessionStyleManifest, root: &str, diagnostics: &mut Vec<Diagnostic>) {
    let valid = matches!(
        (manifest.kind, manifest.built_in_semantic),
        (StyleKind::BuiltIn, Some(_)) | (StyleKind::Custom, None)
    );
    if !valid {
        diagnostics.push(error(
            "STYLE006",
            format!("{root}.built_in_semantic"),
            "style kind and built-in semantic are inconsistent",
            "built-in styles require a semantic; custom styles must omit it",
        ));
    }
}

fn validate_collections(
    manifest: &SessionStyleManifest,
    limits: StyleCompilerLimits,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if manifest.interceptors.len() > limits.max_interceptors {
        diagnostics.push(bounds(root, "interceptors"));
    }
    for (path, values) in [
        ("required_capabilities", &manifest.required_capabilities),
        ("allowed_tool_groups", &manifest.allowed_tool_groups),
        ("allowed_providers", &manifest.allowed_providers),
        ("allowed_plugins", &manifest.allowed_plugins),
        (
            "retry.retryable_failures",
            &manifest.retry.retryable_failures,
        ),
    ] {
        if values.len() > limits.max_collection_items {
            diagnostics.push(bounds(root, path));
        }
        validate_unique_strings(values, &format!("{root}.{path}"), diagnostics);
        for (index, value) in values.iter().enumerate() {
            if !is_name(value) {
                diagnostics.push(error(
                    "STYLE008",
                    format!("{root}.{path}[{index}]"),
                    "declaration is not a stable lowercase name",
                    "use lowercase letters, digits, dots, colons, underscores, or hyphens",
                ));
            }
        }
    }
}

fn validate_availability(
    manifest: &SessionStyleManifest,
    context: &CompileContext,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for (index, capability) in manifest.required_capabilities.iter().enumerate() {
        if !context.capabilities.contains(capability) {
            diagnostics.push(error(
                "STYLE010",
                format!("{root}.required_capabilities[{index}]"),
                format!("runtime capability `{capability}` is unavailable"),
                "enable the capability or select another style",
            ));
        }
    }
    for (index, group) in manifest.allowed_tool_groups.iter().enumerate() {
        if !context.tool_groups.contains_key(group) {
            diagnostics.push(error(
                "STYLE011",
                format!("{root}.allowed_tool_groups[{index}]"),
                format!("tool group `{group}` is unavailable"),
                "install/activate the tool host or remove the group",
            ));
        }
    }
    for (index, provider) in manifest.allowed_providers.iter().enumerate() {
        if !context.providers.contains(provider) {
            diagnostics.push(error(
                "STYLE012",
                format!("{root}.allowed_providers[{index}]"),
                format!("provider `{provider}` is unavailable"),
                "configure the provider or remove it",
            ));
        }
    }
    for (index, plugin) in manifest.allowed_plugins.iter().enumerate() {
        if !context.plugins.contains(plugin) {
            diagnostics.push(error(
                "STYLE013",
                format!("{root}.allowed_plugins[{index}]"),
                format!("plugin `{plugin}` is unavailable"),
                "activate a compatible plugin or remove it",
            ));
        }
    }
}

fn validate_interceptors(
    manifest: &SessionStyleManifest,
    context: &CompileContext,
    limits: StyleCompilerLimits,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for (index, interceptor) in manifest.interceptors.iter().enumerate() {
        let path = format!("{root}.interceptors[{index}]");
        if !is_name(&interceptor.id)
            || !is_name(&interceptor.owner)
            || !is_event(&interceptor.event)
        {
            diagnostics.push(error(
                "STYLE014",
                &path,
                "interceptor identity, owner, or event is invalid",
                "use stable lowercase IDs and a dotted proposal event",
            ));
        }
        for values in [&interceptor.before, &interceptor.after] {
            if values.len() > limits.max_collection_items {
                diagnostics.push(bounds(&path, "ordering"));
            }
        }
        validate_unique_strings(&interceptor.before, &format!("{path}.before"), diagnostics);
        validate_unique_strings(&interceptor.after, &format!("{path}.after"), diagnostics);
        validate_unique_values(
            &interceptor.supported_decisions,
            &format!("{path}.supported_decisions"),
            diagnostics,
        );
        if interceptor.supported_decisions.is_empty() {
            diagnostics.push(error(
                "STYLE015",
                format!("{path}.supported_decisions"),
                "interceptor declares no decision capability",
                "declare at least continue, reject, or another supported decision",
            ));
        }
        for (decision_index, decision) in interceptor.supported_decisions.iter().enumerate() {
            if !context.supported_decisions.contains(decision) {
                diagnostics.push(error(
                    "STYLE015",
                    format!("{path}.supported_decisions[{decision_index}]"),
                    format!("decision `{decision:?}` is unsupported"),
                    "remove the decision or enable its runtime action capability",
                ));
            }
            validate_decision_semantics(
                *decision,
                manifest,
                &format!("{path}.supported_decisions[{decision_index}]"),
                diagnostics,
            );
        }
        validate_unique_strings(
            &interceptor.required_capabilities,
            &format!("{path}.required_capabilities"),
            diagnostics,
        );
        for (capability_index, capability) in interceptor.required_capabilities.iter().enumerate() {
            if !context.capabilities.contains(capability) {
                diagnostics.push(error(
                    "STYLE010",
                    format!("{path}.required_capabilities[{capability_index}]"),
                    format!("interceptor capability `{capability}` is unavailable"),
                    "enable the capability or remove the interceptor",
                ));
            }
        }
        let runtime_owned =
            interceptor.owner == "runtime" || interceptor.owner.starts_with("runtime.");
        if !runtime_owned
            && (!manifest.allowed_plugins.contains(&interceptor.owner)
                || !context.plugins.contains(&interceptor.owner))
        {
            diagnostics.push(error(
                "STYLE013",
                format!("{path}.owner"),
                format!(
                    "interceptor owner `{}` is not an allowed active plugin",
                    interceptor.owner
                ),
                "activate and allow the plugin, or use a runtime-owned interceptor",
            ));
        }
    }
}

fn validate_decision_semantics(
    decision: DecisionCapability,
    manifest: &SessionStyleManifest,
    path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let required = match decision {
        DecisionCapability::RequireApproval => Some("approval"),
        DecisionCapability::Defer => Some("continuations"),
        DecisionCapability::Fork => Some("fork"),
        _ => None,
    };
    if let Some(capability) = required
        && !manifest
            .required_capabilities
            .iter()
            .any(|item| item == capability)
    {
        diagnostics.push(error(
            "STYLE028",
            path,
            format!("decision requires undeclared style capability `{capability}`"),
            "declare the capability or remove the decision",
        ));
    }
    if decision == DecisionCapability::Fork && manifest.child_agents.max_children == 0 {
        diagnostics.push(error(
            "STYLE028",
            path,
            "fork decision is incompatible with disabled child agents",
            "allow bounded child agents or remove fork",
        ));
    }
}

fn compile_interceptors(
    manifest: &SessionStyleManifest,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Vec<String>> {
    let specifications: Vec<_> = manifest.interceptors.iter().map(map_ordering).collect();
    match compile_order(&specifications) {
        Ok(compiled) => Some(
            compiled
                .handlers()
                .iter()
                .map(|handler| handler.as_str().to_owned())
                .collect(),
        ),
        Err(failure) => {
            diagnostics.extend(
                failure
                    .diagnostics()
                    .iter()
                    .map(|diagnostic| map_ordering_error(diagnostic, root)),
            );
            None
        }
    }
}

fn map_ordering(interceptor: &InterceptorDeclaration) -> OrderingSpec {
    let mut specification = OrderingSpec::new(&*interceptor.id, &*interceptor.owner)
        .with_stage(interceptor.stage)
        .with_priority(interceptor.priority);
    for target in &interceptor.before {
        specification = specification.before(&**target);
    }
    for target in &interceptor.after {
        specification = specification.after(&**target);
    }
    specification
}

fn map_ordering_error(diagnostic: &CompileDiagnostic, root: &str) -> Diagnostic {
    match diagnostic {
        CompileDiagnostic::DuplicateHandler { handler, .. } => error(
            "STYLE016",
            format!("{root}.interceptors[{}]", handler.as_str()),
            format!("duplicate interceptor ID `{handler}`"),
            "interceptor IDs must be unique",
        ),
        CompileDiagnostic::MissingDependency {
            handler,
            missing,
            relation,
        } => error(
            "STYLE016",
            format!("{root}.interceptors[{}].{relation}", handler.as_str()),
            format!("ordering references missing interceptor `{missing}`"),
            "add the interceptor or remove the ordering constraint",
        ),
        CompileDiagnostic::OrderingCycle { path } => error(
            "STYLE016",
            format!("{root}.interceptors.ordering"),
            format!(
                "interceptor ordering contains cycle: {}",
                path.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ),
            "remove or reverse an ordering constraint",
        ),
    }
}

fn validate_memory(
    memory: &MemorySelection,
    context: &CompileContext,
    limits: StyleCompilerLimits,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    validate_unique_values(
        &memory.scopes,
        &format!("{root}.memory.scopes"),
        diagnostics,
    );
    let disabled = memory.provider == "none";
    let valid_bounds = memory.max_items <= limits.max_memory_items
        && memory.max_injected_bytes <= limits.max_memory_bytes;
    let valid_disabled = !disabled
        || (memory.scopes.is_empty() && memory.max_items == 0 && memory.max_injected_bytes == 0);
    let valid_enabled = disabled
        || (context.memory_providers.contains(&memory.provider)
            && memory.max_items > 0
            && memory.max_injected_bytes > 0);
    if !is_name(&memory.provider) || !valid_bounds || !valid_disabled || !valid_enabled {
        diagnostics.push(error(
            "STYLE017",
            format!("{root}.memory"),
            "memory selection is unavailable, inconsistent, or exceeds bounds",
            "use none with zero limits, or an available provider with positive bounded limits",
        ));
    }
    let retrieves = memory.retrieval_timing != MemoryRetrievalTiming::Never;
    let valid_disabled_controls = !disabled
        || (memory.retrieval_timing == MemoryRetrievalTiming::Never
            && memory.write_policy == MemoryWritePolicy::Never
            && memory.injection_location == MemoryInjectionLocation::None);
    let valid_retrieval_controls = !retrieves
        || (memory.injection_location != MemoryInjectionLocation::None
            && memory.query.max_query_bytes > 0
            && u64::from(memory.query.max_query_bytes) <= limits.max_memory_bytes);
    let valid_injection = retrieves || memory.injection_location == MemoryInjectionLocation::None;
    if !valid_disabled_controls || !valid_retrieval_controls || !valid_injection {
        diagnostics.push(error(
            "STYLE030",
            format!("{root}.memory"),
            "memory lifecycle controls are inconsistent or exceed bounds",
            "use never/never/none for disabled memory; active retrieval requires a bounded query and injection location",
        ));
    }
}

fn validate_compaction(
    compaction: &CompactionSelection,
    context: &CompileContext,
    max_tokens: u64,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    validate_unique_values(
        &compaction.preservation_requirements,
        &format!("{root}.compaction.preservation_requirements"),
        diagnostics,
    );
    let strategy = compaction_name(compaction.strategy);
    let disabled = compaction.strategy == CompactionStrategy::None;
    let valid_trigger = if disabled {
        compaction.trigger_tokens.is_none()
    } else {
        compaction
            .trigger_tokens
            .is_some_and(|value| value > 0 && value <= max_tokens)
    };
    if !context.compaction_strategies.contains(strategy)
        || !valid_trigger
        || (!disabled
            && (!compaction.preserve_unresolved_tasks || !compaction.preserve_active_processes))
    {
        diagnostics.push(error(
            "STYLE018",
            format!("{root}.compaction"),
            "compaction selection is unavailable, unbounded, or drops required live state",
            "use an available strategy, bounded trigger, and preserve tasks/process state",
        ));
    }
    let valid_disabled_controls = !disabled
        || (compaction.reserved_context_tokens == 0
            && compaction.max_provider_projection_tokens == 0);
    let valid_projection_bound = compaction.max_provider_projection_tokens == 0
        || (compaction.max_provider_projection_tokens <= max_tokens
            && compaction.reserved_context_tokens < compaction.max_provider_projection_tokens);
    let valid_unbounded_projection =
        compaction.max_provider_projection_tokens != 0 || compaction.reserved_context_tokens == 0;
    if !valid_disabled_controls || !valid_projection_bound || !valid_unbounded_projection {
        diagnostics.push(error(
            "STYLE031",
            format!("{root}.compaction"),
            "compaction context budgets are inconsistent with the provider projection bound",
            "use zero context controls for none, or reserve fewer tokens than a bounded projection within the style token budget",
        ));
    }
}

fn validate_approvals(approvals: &ApprovalDefaults, root: &str, diagnostics: &mut Vec<Diagnostic>) {
    for group in approvals.groups.keys() {
        if !is_name(group) {
            diagnostics.push(error(
                "STYLE019",
                format!("{root}.approvals.groups[{group}]"),
                "approval group is invalid",
                "use a stable lowercase action or tool group",
            ));
        }
    }
}

fn validate_budgets(
    budgets: ExecutionBudgets,
    limits: StyleCompilerLimits,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for (field, value, maximum) in [
        (
            "max_iterations",
            u64::from(budgets.max_iterations),
            u64::from(limits.max_iterations),
        ),
        ("max_steps", budgets.max_steps, limits.max_steps),
        ("max_tokens", budgets.max_tokens, limits.max_tokens),
        (
            "max_cost_micros",
            budgets.max_cost_micros,
            limits.max_cost_micros,
        ),
        (
            "max_duration_ms",
            budgets.max_duration_ms,
            limits.max_duration_ms,
        ),
    ] {
        if value == 0 || value > maximum {
            diagnostics.push(error(
                "STYLE020",
                format!("{root}.budgets.{field}"),
                format!("budget is {value}; valid range is 1..={maximum}"),
                "declare a positive hard bound within runtime policy",
            ));
        }
    }
}

fn validate_children(
    children: &ChildAgentLimits,
    budgets: ExecutionBudgets,
    required_capabilities: &[String],
    allowed_tool_groups: &[String],
    limits: StyleCompilerLimits,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let disabled = children.max_children == 0;
    let valid_disabled = !disabled
        || (children.max_concurrent == 0
            && children.max_depth == 0
            && children.per_child_token_budget == 0
            && children.child_style.is_none()
            && children.workspace_mode.is_none()
            && children.custom_workspace.is_none()
            && children.inherit_provider.is_none()
            && children.inherit_model.is_none()
            && children.context_budget_tokens.is_none()
            && children.per_child_cost_budget_micros.is_none()
            && children.tool_groups.is_empty()
            && children.memory_access.is_none()
            && children.join_behavior.is_none()
            && children.cancellation_behavior.is_none()
            && children.reviewer_max_attempts.is_none());
    let child_style_valid = children.child_style.as_deref().is_some_and(|selector| {
        selector.split_once('@').is_some_and(|(id, version)| {
            !id.trim().is_empty()
                && Version::parse(version).is_ok()
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    });
    let workspace_valid = match (
        children.workspace_mode,
        children.custom_workspace.as_deref(),
    ) {
        (Some(ChildWorkspaceMode::ExplicitCustomWorkspace), Some(path)) => !path.trim().is_empty(),
        (Some(mode), None) => mode != ChildWorkspaceMode::ExplicitCustomWorkspace,
        (None | Some(_), Some(_)) | (None, None) => false,
    };
    let tool_groups_valid = children.tool_groups.iter().all(|group| {
        !group.trim().is_empty() && allowed_tool_groups.iter().any(|allowed| allowed == group)
    });
    let valid_enabled = disabled
        || (children.max_children <= limits.max_children
            && children.max_concurrent > 0
            && children.max_concurrent <= children.max_children
            && children.max_concurrent <= limits.max_concurrent_children
            && children.max_depth > 0
            && children.max_depth <= limits.max_child_depth
            && children.per_child_token_budget > 0
            && children.per_child_token_budget <= budgets.max_tokens
            && child_style_valid
            && workspace_valid
            && children.inherit_provider.is_some()
            && children.inherit_model.is_some()
            && children
                .context_budget_tokens
                .is_some_and(|budget| budget > 0 && budget <= children.per_child_token_budget)
            && children
                .per_child_cost_budget_micros
                .is_some_and(|budget| budget > 0 && budget <= budgets.max_cost_micros)
            && tool_groups_valid
            && children.memory_access.is_some()
            && children.join_behavior.is_some()
            && children.cancellation_behavior.is_some()
            && children
                .reviewer_max_attempts
                .is_some_and(|attempts| attempts > 0 && attempts <= budgets.max_iterations));
    let capability_declared = disabled || required_capabilities.iter().any(|item| item == "agents");
    if !valid_disabled || !valid_enabled || !capability_declared {
        diagnostics.push(error(
            "STYLE021",
            format!("{root}.child_agents"),
            "child-agent limits are inconsistent or exceed style/runtime budgets",
            "use all zeros to disable children, or positive nested bounds within policy",
        ));
    }
}

fn validate_retry(
    retry: &RetryPolicy,
    budgets: ExecutionBudgets,
    limits: StyleCompilerLimits,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let valid = (1..=limits.max_retry_attempts).contains(&retry.max_attempts)
        && retry.initial_backoff_ms <= retry.max_backoff_ms
        && retry.max_backoff_ms < budgets.max_duration_ms
        && (retry.max_attempts == 1 || !retry.retryable_failures.is_empty());
    if !valid {
        diagnostics.push(error(
            "STYLE022",
            format!("{root}.retry"),
            "retry policy is unbounded, inconsistent, or has no retryable failures",
            "bound attempts/backoff and declare retryable failures when attempts exceed one",
        ));
    }
}

fn validate_termination(
    termination: &TerminationPolicy,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    validate_unique_values(
        &termination.allowed_outcomes,
        &format!("{root}.termination.allowed_outcomes"),
        diagnostics,
    );
    if termination.allowed_outcomes.is_empty()
        || !termination
            .allowed_outcomes
            .contains(&termination.on_hard_limit)
        || !termination.require_explicit_terminal_node
    {
        diagnostics.push(error(
            "STYLE023",
            format!("{root}.termination"),
            "termination is implicit or hard-limit outcome is not allowed",
            "require terminal nodes and include a bounded hard-limit outcome",
        ));
    }
}

fn validate_selection(selection: TopLevelSelection, root: &str, diagnostics: &mut Vec<Diagnostic>) {
    if !selection.requires_explicit_selection || selection.model_may_select {
        diagnostics.push(error(
            "STYLE024",
            format!("{root}.selection"),
            "top-level session styles must be explicitly selected outside the model",
            "require explicit selection and disable model selection",
        ));
    }
}

fn resolve_graph(
    manifest: &SessionStyleManifest,
    context: &CompileContext,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<String> {
    match &manifest.graph {
        GraphSource::Inline { source } if !source.trim().is_empty() => Some(source.clone()),
        GraphSource::Inline { .. } => {
            diagnostics.push(error(
                "STYLE007",
                format!("{root}.graph.source"),
                "inline graph source is empty",
                "supply a versioned graph",
            ));
            None
        }
        GraphSource::Reference { id, content_hash } => {
            if !is_name(id) {
                diagnostics.push(error(
                    "STYLE007",
                    format!("{root}.graph.id"),
                    "graph reference ID is invalid",
                    "use a stable lowercase reference ID",
                ));
                return None;
            }
            let Ok(expected) = ContentHash::from_str(content_hash) else {
                diagnostics.push(error(
                    "STYLE027",
                    format!("{root}.graph.content_hash"),
                    "graph content hash is not lowercase BLAKE3 hexadecimal",
                    "supply the exact referenced graph digest",
                ));
                return None;
            };
            if expected.to_hex() != *content_hash {
                diagnostics.push(error(
                    "STYLE027",
                    format!("{root}.graph.content_hash"),
                    "graph content hash must use canonical lowercase hexadecimal",
                    "use the lowercase BLAKE3 digest",
                ));
                return None;
            }
            let Some(source) = context.graph_references.get(id) else {
                diagnostics.push(error(
                    "STYLE007",
                    format!("{root}.graph.id"),
                    format!("graph reference `{id}` is unavailable"),
                    "provide the referenced graph source",
                ));
                return None;
            };
            if ContentHash::digest(source.as_bytes()) != expected {
                diagnostics.push(error(
                    "STYLE027",
                    format!("{root}.graph.content_hash"),
                    "referenced graph does not match its content hash",
                    "update the reference or restore the expected content",
                ));
                return None;
            }
            Some(source.clone())
        }
    }
}

fn compile_and_validate_graph(
    source: &str,
    manifest: &SessionStyleManifest,
    context: &CompileContext,
    limits: StyleCompilerLimits,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<ExecutableGraph> {
    let definition = match GraphDefinition::parse(source, limits.graph) {
        Ok(definition) => definition,
        Err(error) => {
            diagnostics.push(graph_error(root, &error));
            return None;
        }
    };
    validate_graph_availability(&definition, manifest, context, root, diagnostics);
    validate_graph_termination(&definition, &manifest.termination, root, diagnostics);
    match compile_graph(
        source,
        &GraphCacheInputs {
            plugin_set_hash: context.plugin_set_hash,
            runtime_api_version: context.runtime_api_version.clone(),
            capability_set: context.capabilities.clone(),
        },
        limits.graph,
    ) {
        Ok(graph) => Some(graph),
        Err(error) => {
            diagnostics.push(graph_error(root, &error));
            None
        }
    }
}

fn validate_graph_availability(
    definition: &GraphDefinition,
    manifest: &SessionStyleManifest,
    context: &CompileContext,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let graph_budget = definition.budget;
    let style_budget = manifest.budgets;
    for (field, graph_value, style_value) in [
        ("max_steps", graph_budget.max_steps, style_budget.max_steps),
        (
            "max_tokens",
            graph_budget.max_tokens,
            style_budget.max_tokens,
        ),
        (
            "max_cost_micros",
            graph_budget.max_cost_micros,
            style_budget.max_cost_micros,
        ),
        (
            "max_duration_ms",
            graph_budget.max_duration_ms,
            style_budget.max_duration_ms,
        ),
    ] {
        if graph_value > style_value {
            diagnostics.push(error(
                "STYLE026",
                format!("{root}.graph.budget.{field}"),
                format!("graph budget {graph_value} exceeds style budget {style_value}"),
                "reduce the graph budget or explicitly raise the bounded style budget",
            ));
        }
    }
    for node in &definition.nodes {
        if node
            .max_iterations
            .is_some_and(|iterations| iterations > manifest.budgets.max_iterations)
        {
            diagnostics.push(error(
                "STYLE026",
                format!("{root}.graph.nodes[{}].max_iterations", node.id),
                "node iteration bound exceeds the style iteration budget",
                "reduce the loop bound",
            ));
        }
        if node.retry_limit >= manifest.retry.max_attempts {
            diagnostics.push(error(
                "STYLE026",
                format!("{root}.graph.nodes[{}].retry_limit", node.id),
                "node retry count is not below the style total-attempt bound",
                "reduce node retries or raise the bounded style retry policy",
            ));
        }
    }
    let allowed_tools: BTreeSet<_> = manifest
        .allowed_tool_groups
        .iter()
        .filter_map(|group| context.tool_groups.get(group))
        .flatten()
        .cloned()
        .collect();
    for tool in &definition.declarations.tools {
        if !allowed_tools.contains(tool) {
            diagnostics.push(error(
                "STYLE026",
                format!("{root}.graph.declarations.tools[{tool}]"),
                format!("graph tool `{tool}` is unavailable or outside allowed groups"),
                "activate and allow the tool group containing this tool",
            ));
        }
    }
    for provider in &definition.declarations.providers {
        if !context.providers.contains(provider) || !manifest.allowed_providers.contains(provider) {
            diagnostics.push(error(
                "STYLE012",
                format!("{root}.graph.declarations.providers[{provider}]"),
                format!("graph provider `{provider}` is unavailable or not allowed"),
                "activate and allow the provider",
            ));
        }
    }
    for capability in &definition.declarations.capabilities {
        if !manifest.required_capabilities.contains(capability) {
            diagnostics.push(error(
                "STYLE026",
                format!("{root}.graph.declarations.capabilities[{capability}]"),
                format!("graph capability `{capability}` is not declared by the style"),
                "add it to required_capabilities",
            ));
        }
    }
}

fn validate_graph_termination(
    definition: &GraphDefinition,
    termination: &TerminationPolicy,
    root: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for node in &definition.nodes {
        let outcome = match node.kind {
            NodeKind::CompleteTurn => Some(TerminationOutcome::CompleteTurn),
            NodeKind::CompleteSession => Some(TerminationOutcome::CompleteSession),
            NodeKind::Fail => Some(TerminationOutcome::Fail),
            _ => None,
        };
        if let Some(outcome) = outcome
            && !termination.allowed_outcomes.contains(&outcome)
        {
            diagnostics.push(error(
                "STYLE023",
                format!("{root}.graph.nodes[{}]", node.id),
                format!("terminal outcome `{outcome:?}` is not allowed by the style"),
                "add the outcome to termination.allowed_outcomes or change the terminal node",
            ));
        }
    }
}

fn graph_error(root: &str, error_value: &GraphError) -> Diagnostic {
    error(
        "STYLE025",
        format!("{root}.graph"),
        error_value.to_string(),
        "fix the graph structure, bounds, declarations, cycles, or parallel writes",
    )
}

fn build_cache_key(
    manifest: &SessionStyleManifest,
    context: &CompileContext,
) -> Result<StyleCacheKey, serde_json::Error> {
    // Bind the cache key to the exact canonical manifest representation
    // returned by `to_json` and retained in session style locks.
    let style = ContentHash::digest(&serde_json::to_vec_pretty(manifest)?);
    let runtime = ContentHash::digest(context.runtime_api_version.as_bytes());
    let capabilities = ContentHash::digest(&encode_strings(context.capabilities.iter()));
    let mut combined = Vec::new();
    for hash in [style, context.plugin_set_hash, runtime, capabilities] {
        combined.extend_from_slice(hash.as_bytes());
    }
    Ok(StyleCacheKey {
        style_content_hash: style,
        plugin_set_hash: context.plugin_set_hash,
        runtime_api_hash: runtime,
        capability_set_hash: capabilities,
        combined_hash: ContentHash::digest(&combined),
    })
}

fn encode_strings<'a>(values: impl Iterator<Item = &'a String>) -> Vec<u8> {
    let mut encoded = Vec::new();
    for value in values {
        encoded.extend_from_slice(&(value.len() as u64).to_le_bytes());
        encoded.extend_from_slice(value.as_bytes());
    }
    encoded
}

fn compaction_name(strategy: CompactionStrategy) -> &'static str {
    match strategy {
        CompactionStrategy::SlidingWindow => "sliding_window",
        CompactionStrategy::Summary => "summary",
        CompactionStrategy::ArtifactHandoff => "artifact_handoff",
        CompactionStrategy::ToolOutputEviction => "tool_output_eviction",
        CompactionStrategy::None => "none",
    }
}

fn validate_unique_strings(values: &[String], path: &str, diagnostics: &mut Vec<Diagnostic>) {
    let mut seen = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        if !seen.insert(value) {
            diagnostics.push(error(
                "STYLE008",
                format!("{path}[{index}]"),
                format!("duplicate declaration `{value}`"),
                "remove the duplicate declaration",
            ));
        }
    }
}

fn validate_unique_values<T>(values: &[T], path: &str, diagnostics: &mut Vec<Diagnostic>)
where
    T: Ord + fmt::Debug,
{
    let mut seen = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        if !seen.insert(value) {
            diagnostics.push(error(
                "STYLE008",
                format!("{path}[{index}]"),
                format!("duplicate declaration `{value:?}`"),
                "remove the duplicate declaration",
            ));
        }
    }
}

fn sorted(values: &[String]) -> Vec<String> {
    let mut values = values.to_vec();
    values.sort();
    values
}

fn is_name(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && !value.starts_with(['.', ':', '-', '_'])
        && !value.ends_with(['.', ':', '-', '_'])
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".:_-".contains(&byte)
        })
}

fn is_event(value: &str) -> bool {
    is_name(value) && value.contains('.')
}

fn bounds(root: &str, field: &str) -> Diagnostic {
    error(
        "STYLE009",
        format!("{root}.{field}"),
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
