//! SDK-owned component selection transforms for per-session style bindings.

use std::collections::BTreeSet;

use agentmod_graph_engine::{CompilerLimits as GraphCompilerLimits, GraphDefinition};

use crate::GraphSource;
use crate::{
    ChildMemoryAccess, CompactionStrategy, ExecutionBudgets, MemoryInjectionLocation,
    MemoryQueryConstruction, MemoryRetrievalTiming, MemoryScope, MemoryWritePolicy,
    SessionStyleManifest,
};

/// Optional caller-selected hard limits for one immutable session binding.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionBudgetOverrides {
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

impl ExecutionBudgetOverrides {
    /// Returns whether no field changes the base manifest.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.max_iterations.is_none()
            && self.max_steps.is_none()
            && self.max_tokens.is_none()
            && self.max_cost_micros.is_none()
            && self.max_duration_ms.is_none()
    }
}

/// Applies the complete immutable restriction set inherited by one child
/// session before compilation and execution-plan binding.
///
/// This transform deliberately updates the manifest, rather than only a
/// runtime binding projection, so restart compatibility checks can recompile
/// the exact retained descriptor without silently rebinding the child.
///
/// # Errors
///
/// Returns [`ComponentSelectionError`] when the selected budgets cannot be
/// applied to the graph manifest.
pub fn select_child_session_restrictions(
    manifest: &mut SessionStyleManifest,
    tool_groups: &BTreeSet<String>,
    memory_access: ChildMemoryAccess,
    budgets: ExecutionBudgetOverrides,
    inherited_provider: Option<&str>,
) -> Result<(), ComponentSelectionError> {
    manifest
        .allowed_tool_groups
        .retain(|group| tool_groups.contains(group));
    match memory_access {
        ChildMemoryAccess::None => select_memory_provider(manifest, "none"),
        ChildMemoryAccess::ReadOnly => {
            manifest.memory.write_policy = MemoryWritePolicy::Never;
        }
        ChildMemoryAccess::ReadWrite => {}
    }
    if let Some(provider) = inherited_provider {
        select_inherited_provider(manifest, provider)?;
    }
    if budgets.is_empty() {
        Ok(())
    } else {
        select_execution_budgets(manifest, budgets)
    }
}

fn select_inherited_provider(
    manifest: &mut SessionStyleManifest,
    provider: &str,
) -> Result<(), ComponentSelectionError> {
    if provider.is_empty()
        || provider.len() > 128
        || !provider.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        return Err(ComponentSelectionError::InvalidInheritedProvider(
            provider.to_owned(),
        ));
    }
    let GraphSource::Inline { source } = &mut manifest.graph else {
        return Err(ComponentSelectionError::InvalidGraph(String::from(
            "provider inheritance requires an inline graph",
        )));
    };
    let mut graph = GraphDefinition::parse(source, GraphCompilerLimits::default())
        .map_err(|error| ComponentSelectionError::InvalidGraph(error.to_string()))?;
    graph.declarations.providers.clear();
    graph.declarations.providers.insert(provider.to_owned());
    for node in &mut graph.nodes {
        if matches!(
            node.kind,
            agentmod_graph_engine::NodeKind::ModelCall | agentmod_graph_engine::NodeKind::Review
        ) {
            node.provider = Some(provider.to_owned());
        }
    }
    manifest.allowed_providers = vec![provider.to_owned()];
    *source = toml::to_string(&graph)
        .map_err(|error| ComponentSelectionError::InvalidGraph(error.to_string()))?;
    Ok(())
}

/// Applies a memory-provider override while retaining the style's lifecycle
/// policy whenever it already selected active memory.
pub fn select_memory_provider(manifest: &mut SessionStyleManifest, provider: &str) {
    manifest.memory.plugin = None;
    if provider == "none" {
        manifest.memory.provider = String::from("none");
        manifest.memory.scopes.clear();
        manifest.memory.retrieval_timing = MemoryRetrievalTiming::Never;
        manifest.memory.query = MemoryQueryConstruction::default();
        manifest.memory.max_items = 0;
        manifest.memory.max_injected_bytes = 0;
        manifest.memory.write_policy = MemoryWritePolicy::Never;
        manifest.memory.injection_location = MemoryInjectionLocation::None;
        return;
    }
    if manifest.memory.provider == "none" {
        manifest.memory.scopes = vec![MemoryScope::Session];
        manifest.memory.retrieval_timing = MemoryRetrievalTiming::TurnStart;
        manifest.memory.query = MemoryQueryConstruction::default();
        manifest.memory.max_items = 32;
        manifest.memory.max_injected_bytes = 256 * 1024;
        manifest.memory.write_policy = MemoryWritePolicy::ExplicitOnly;
        manifest.memory.injection_location = MemoryInjectionLocation::BeforeCurrentInput;
    }
    provider.clone_into(&mut manifest.memory.provider);
}

/// Applies a compaction-strategy override while retaining active style limits
/// or installing conservative bounded controls when enabling compaction.
///
/// # Errors
///
/// Returns [`ComponentSelectionError`] for an identifier outside the SDK's
/// current typed strategy model.
pub fn select_compaction_strategy(
    manifest: &mut SessionStyleManifest,
    strategy: &str,
) -> Result<(), ComponentSelectionError> {
    let strategy = match strategy {
        "none" => CompactionStrategy::None,
        "sliding_window" => CompactionStrategy::SlidingWindow,
        "summary" => CompactionStrategy::Summary,
        "artifact_handoff" => CompactionStrategy::ArtifactHandoff,
        "tool_output_eviction" => CompactionStrategy::ToolOutputEviction,
        _ => {
            return Err(ComponentSelectionError::UnknownCompaction(
                strategy.to_owned(),
            ));
        }
    };
    manifest.compaction.plugin = None;
    if strategy == CompactionStrategy::None {
        manifest.compaction.strategy = strategy;
        manifest.compaction.trigger_tokens = None;
        manifest.compaction.reserved_context_tokens = 0;
        manifest.compaction.max_provider_projection_tokens = 0;
        return Ok(());
    }
    if manifest.compaction.strategy == CompactionStrategy::None {
        let maximum = manifest.budgets.max_tokens;
        manifest.compaction.trigger_tokens = Some((maximum.saturating_mul(3) / 4).max(1));
        manifest.compaction.max_provider_projection_tokens = maximum;
        manifest.compaction.reserved_context_tokens = (maximum / 16).min(32_000);
        manifest.compaction.preserve_unresolved_tasks = true;
        manifest.compaction.preserve_active_processes = true;
    }
    manifest.compaction.strategy = strategy;
    Ok(())
}

/// Applies caller-selected style budgets and narrows every subordinate bound
/// that the SDK requires to remain within those style-wide ceilings.
///
/// The regular SDK compiler remains authoritative for positive/runtime maximum
/// validation. This transform only maintains relationships between the style,
/// graph, compaction, retry, and child-agent declarations.
///
/// # Errors
///
/// Returns [`ComponentSelectionError`] when an already validated inline graph
/// cannot be parsed or serialized for the budget transform.
pub fn select_execution_budgets(
    manifest: &mut SessionStyleManifest,
    overrides: ExecutionBudgetOverrides,
) -> Result<(), ComponentSelectionError> {
    let mut selected = manifest.budgets;
    if let Some(value) = overrides.max_iterations {
        selected.max_iterations = value;
    }
    if let Some(value) = overrides.max_steps {
        selected.max_steps = value;
    }
    if let Some(value) = overrides.max_tokens {
        selected.max_tokens = value;
    }
    if let Some(value) = overrides.max_cost_micros {
        selected.max_cost_micros = value;
    }
    if let Some(value) = overrides.max_duration_ms {
        selected.max_duration_ms = value;
    }
    manifest.budgets = selected;
    narrow_graph_budgets(manifest, selected)?;
    narrow_compaction_budgets(manifest, selected);
    narrow_child_budgets(manifest, selected);
    narrow_retry_budgets(manifest, selected);
    Ok(())
}

fn narrow_graph_budgets(
    manifest: &mut SessionStyleManifest,
    selected: ExecutionBudgets,
) -> Result<(), ComponentSelectionError> {
    let GraphSource::Inline { source } = &mut manifest.graph else {
        return Ok(());
    };
    let mut graph = GraphDefinition::parse(source, GraphCompilerLimits::default())
        .map_err(|error| ComponentSelectionError::InvalidGraph(error.to_string()))?;
    graph.budget.max_steps = graph.budget.max_steps.min(selected.max_steps);
    graph.budget.max_tokens = graph.budget.max_tokens.min(selected.max_tokens);
    graph.budget.max_cost_micros = graph.budget.max_cost_micros.min(selected.max_cost_micros);
    graph.budget.max_duration_ms = graph.budget.max_duration_ms.min(selected.max_duration_ms);
    for node in &mut graph.nodes {
        if let Some(max_iterations) = &mut node.max_iterations {
            *max_iterations = (*max_iterations).min(selected.max_iterations);
        }
    }
    *source = toml::to_string(&graph)
        .map_err(|error| ComponentSelectionError::InvalidGraph(error.to_string()))?;
    Ok(())
}

fn narrow_compaction_budgets(manifest: &mut SessionStyleManifest, selected: ExecutionBudgets) {
    if manifest.compaction.strategy == CompactionStrategy::None {
        return;
    }
    if let Some(trigger) = &mut manifest.compaction.trigger_tokens {
        *trigger = (*trigger).min(selected.max_tokens);
    }
    manifest.compaction.max_provider_projection_tokens = manifest
        .compaction
        .max_provider_projection_tokens
        .min(selected.max_tokens);
    manifest.compaction.reserved_context_tokens = manifest.compaction.reserved_context_tokens.min(
        manifest
            .compaction
            .max_provider_projection_tokens
            .saturating_sub(1),
    );
}

fn narrow_child_budgets(manifest: &mut SessionStyleManifest, selected: ExecutionBudgets) {
    if manifest.child_agents.max_children == 0 {
        return;
    }
    manifest.child_agents.per_child_token_budget = manifest
        .child_agents
        .per_child_token_budget
        .min(selected.max_tokens);
    if let Some(context) = &mut manifest.child_agents.context_budget_tokens {
        *context = (*context).min(manifest.child_agents.per_child_token_budget);
    }
    if let Some(cost) = &mut manifest.child_agents.per_child_cost_budget_micros {
        *cost = (*cost).min(selected.max_cost_micros);
    }
    if let Some(attempts) = &mut manifest.child_agents.reviewer_max_attempts {
        *attempts = (*attempts).min(selected.max_iterations);
    }
}

fn narrow_retry_budgets(manifest: &mut SessionStyleManifest, selected: ExecutionBudgets) {
    let maximum_backoff = selected.max_duration_ms.saturating_sub(1);
    manifest.retry.max_backoff_ms = manifest.retry.max_backoff_ms.min(maximum_backoff);
    manifest.retry.initial_backoff_ms = manifest
        .retry
        .initial_backoff_ms
        .min(manifest.retry.max_backoff_ms);
}

/// Invalid typed component selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentSelectionError {
    /// Compaction remains a closed SDK enum until plugin strategies gain a
    /// versioned protocol representation.
    UnknownCompaction(String),
    /// A previously compiled inline graph could not be transformed.
    InvalidGraph(String),
    /// Inherited provider identity is outside the bounded SDK identifier model.
    InvalidInheritedProvider(String),
}

impl std::fmt::Display for ComponentSelectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCompaction(strategy) => {
                write!(formatter, "unknown compaction strategy `{strategy}`")
            }
            Self::InvalidGraph(detail) => {
                write!(
                    formatter,
                    "style graph cannot accept budget overrides: {detail}"
                )
            }
            Self::InvalidInheritedProvider(provider) => {
                write!(formatter, "inherited provider `{provider}` is invalid")
            }
        }
    }
}

impl std::error::Error for ComponentSelectionError {}
